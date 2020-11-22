#include <cstring>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

class array: public message
{
private:
    int* p;
    size_t size_;
    bool res;
public:

    array(size_t sz, int* pt = nullptr): size_(sz), p(pt)
    {
        if (p == nullptr)
        {
            p = new int[size_];
            res = true;
        }
        else
            res = false;
    }

    ~array()
    {
        if (res)
            delete[] p;
    }

    void send(const sender& se) const override
    { se.send(p, size_); }

    void isend(const sender& se, request_block& req) const override
    { se.isend(p, size_, req); }

    void recv(const receiver& re) override
    { re.recv(p, size_); }

    void irecv(const receiver& re, request_block& req) override
    { re.irecv(p, size_, req); }

    int& operator[](size_t n)
    { return p[n]; }

    const int& operator[](size_t n) const
    { return p[n]; }

    size_t size() const
    { return size_; }

    int* data()
    { return p; }
};

class merge_task: public task
{
public:
    merge_task(): task()
    { }
    void operator()(const array& s1, const array& s2, array& out)
    {
        size_t first = 0, second = 0;
        for (size_t i = 0; i < out.size(); ++i)
        {
            if ((first >= s1.size()))
                out[i] = s2[second++];
            else if ((second < s2.size()) && (s2[second] < s1[first]))
                out[i] = s2[second++];
            else
                out[i] = s1[first++];
        }
    }
};

class merge_all_task: public task
{
public:
    merge_all_task(): task()
    { }
    void operator()(array& s1, array& s2)
    {
        for (size_t i = 0; i < s1.size(); ++i)
            s2[i] = s1[i];
        merge_it(&s1[0], &s2[0], s1.size()/2, s1.size());
    }
    void merge_it(int* s, int* out, size_t size1, size_t size2)
    {
        if (size2 < 2)
        {
            for (size_t i = 0; i < size2; ++i)
                out[i] = s[i];
            return;
        }
        merge_it(out, s, size1 / 2, size1);
        merge_it(out + size1, s + size1, (size2 - size1) / 2, size2 - size1);
        size_t first = 0;
        size_t second = size1;
        for (size_t i = 0; i < size2; ++i)
        {
            if (first == size1)
                out[i] = s[second++];
            else if ((second < size2) && (s[second] < s[first]))
                out[i] = s[second++];
            else
                out[i] = s[first++];
        }
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    size_t layers = 2;
    size_t size = 100000;
    bool checks = false;
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            size = atoll(argv[++i]);
        }
        else if ((strcmp(argv[i], "-l") == 0) || (strcmp(argv[i], "-layers") == 0))
        {
            layers = atoll(argv[++i]);
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            checks = true;
        }
    }

    int* p1 = new int[size];
    int* p2 = new int[size];
    int* p3;
    std::mt19937 mt(static_cast<unsigned>(time(0)));
    std::uniform_int_distribution<int> uid(0, 10000);
    for (size_t i = 0; i < size; ++i)
        p1[i] = uid(mt);

    if (checks)
    {
        p3 = new int[size];
        for (size_t i = 0; i < size; ++i)
            p3[i] = p1[i];
    }
    
    double true_start_time = MPI_Wtime();
    parallelizer pz;
    task_graph tg;

    int comm_size = pz.get_proc_count();
    {
        int j = 1;
        size_t i = 0;
        while (j < comm_size)
        {
            j <<= 1;
            ++i;
        }
        layers = i;
    }

    std::vector<message*> fin, tmp;
    std::vector<std::vector<message*>> args, args2;
    std::vector<task*> v1, v2;
    if (layers != 0)
    {
        if (layers % 2 == 0)
            std::swap(p1,p2);
        array* arr1 = new array(size / 2, p2);
        array* arr2 = new array(size - size / 2, p2 + size / 2);
        array* arr_p1 = new array(size, p1);

        v2.push_back(new merge_task());
        args.push_back({arr1, arr2, arr_p1});

        tg.add_task<merge_task, const array, const array, array>(v2.back(), args.back());

        fin.push_back(arr_p1);
        for (size_t i = 1; i < layers; ++i)
        {
            size_t q = 1ull << i;
            v1.resize(q);
            args2.resize(q);
            for (size_t j = 0; j < q; ++j)
            {
                int* ptr;
                if (j%2)
                {
                    arr_p1 = (array*)args[j / 2][1];
                    ptr = ((array*)args[j / 2][2])->data() + ((array*)args[j / 2][0])->size();
                }
                else
                {
                    arr_p1 = (array*)args[j / 2][0];
                    ptr = ((array*)args[j / 2][2])->data();
                }

                arr1 = new array(arr_p1->size() / 2, ptr);

                arr2 = new array(arr_p1->size() - arr_p1->size() / 2, ptr + arr_p1->size() / 2);

                v1[j] = new merge_task();
                args2[j] = {arr1, arr2, arr_p1};
                tg.add_task<merge_task, const array, const array, array>(v1[j], args2[j]);

                tg.add_dependence(v1[j], v2[j/2]);
            }
            swap(v1, v2);
            swap(args, args2);
        }

        for (size_t i = 0; i < v2.size(); ++i)
        {
            arr1 = new array(((array*)args[i][0])->size(), ((array*)args[i][2])->data());
            arr2 = ((array*)args[i][0]);
            task* tt = new merge_all_task();
            tg.add_task<merge_all_task, array, array>(tt, {arr1, arr2});
            tg.add_dependence(tt, v2[i]);

            arr1 = new array(((array*)args[i][1])->size(), ((array*)args[i][2])->data() + ((array*)args[i][0])->size());
            arr2 = ((array*)args[i][1]);
            tt = new merge_all_task();
            tg.add_task<merge_all_task, array, array>(tt, {arr1, arr2});
            tg.add_dependence(tt, v2[i]);
        }
    }
    else
    {
        array* arr1 = new array(size, p1);
        array* arr2 = new array(size, p2);
        tg.add_task<merge_all_task, array, array>(new merge_all_task(), {arr1, arr2});
        std::swap(p1, p2);
        fin.push_back(arr1);
        fin.push_back(arr2);
    }

    pz.init(tg);

    pz.execution();

    if (pz.get_current_proc() == parallelizer::main_proc)
    {
        double dt = MPI_Wtime();
        if (checks)
        {
            std::sort(p3, p3 + size);
            bool fl = false;
            for (size_t i = 0; i < size; ++i)
                if (p1[i] != p3[i])
                    fl = true;
            if (fl)
                std::cout << "wrong\n";
            else
                std::cout << "correct\n";
        }
        std::cout << dt - true_start_time << std::endl;
    }
}
