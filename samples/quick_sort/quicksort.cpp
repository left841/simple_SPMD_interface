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
public:
    bool created;
    int* p;
    size_t size;

    array(size_t sz): size(sz)
    {
        p = new int[size];
        created = true;
    }

    array(const array& m, size_t sz, size_t offset): size(sz)
    {
        p = m.p + offset;
        created = false;
    }

    array(size_t sz, size_t offset): size(sz)
    {
        p = new int[size];
        created = true;
    }

    void include(const array& child, size_t sz, size_t offset)
    {
        if (child.created)
        {
            int* q = p + offset;
            for (size_t i = 0; i < child.size; ++i)
                q[i] = child.p[i];
        }
    }

    ~array()
    {
        if (created)
            delete[] p;
    }

    int& operator[](size_t n)
    { return p[n]; }

    const int& operator[](size_t n) const
    { return p[n]; }

    void send(const sender& se) const override
    { se.send(p, size); }

    void recv(const receiver& re) override
    { re.recv(p, size); }

    void isend(const sender& se, request_block& req) const override
    { se.isend(p, size, req); }

    void irecv(const receiver& re, request_block& req) override
    { re.irecv(p, size, req); }
};

bool checking = false;
bool pred_initialized = false;

class quick_task: public task
{
private:

    void simple_quicksort(int* a, size_t size)
    {
        int bel;
        int mi = std::min(std::min(a[0], a[size - 1]), a[size / 2]);
        int ma = std::max(std::max(a[0], a[size - 1]), a[size / 2]);

        if (size < 2)
            return;

        if ((a[0] > mi) && (a[0] < ma))
            bel = a[0];
        else if ((a[size - 1] > mi) && (a[size - 1] < ma))
            bel = a[size - 1];
        else
            bel = a[size / 2];

        size_t l = 0, r = size - 1;
        while (l <= r)
        {
            while (a[l] < bel)
                ++l;
            while (a[r] > bel)
                --r;
            if (l <= r)
                std::swap(a[l++], a[r--]);
        }

        simple_quicksort(a, r + 1);
        simple_quicksort(a + r + 1, size - (r + 1));
    }

public:
    static size_t pred;

    void operator()(array& a)
    {
        size_t sz = a.size;
        if (sz < pred)
            simple_quicksort(a.p, sz);
        else
        {
            int bel;
            int mi = std::min(std::min(a[0], a[sz - 1]), a[sz / 2]);
            int ma = std::max(std::max(a[0], a[sz - 1]), a[sz / 2]);
            if ((a[0] > mi) && (a[0] < ma))
                bel = a[0];
            else if ((a[sz - 1] > mi) && (a[sz - 1] < ma))
                bel = a[sz - 1];
            else
                bel = a[sz / 2];

            size_t l = 0, r = sz - 1;
            while (l <= r)
            {
                while (a[l] < bel)
                    ++l;
                while (a[r] > bel)
                    --r;
                if (l <= r)
                    std::swap(a[l++], a[r--]);
            }

            if (r + 1 > 1)
            {
                mes_id<array> p1 = create_message_child<array>(arg_id<0, array>(), new size_t(r + 1), new size_t(0));
                create_child_task<quick_task>(p1);
            }

            if (sz - (r + 1) > 1)
            {
                mes_id<array> p2 = create_message_child<array>(arg_id<0, array>(), new size_t(sz - r - 1), new size_t(r + 1));
                create_child_task<quick_task>(p2);
            }
        }
    }
};

size_t quick_task::pred = 1000;

class check_task;

class init_task: public task
{
public:
    void operator()(size_t size, double& t)
    {
        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(0, 10000);

        array& a1 = *new array(size);
        mes_id<array> a1_id = add_message(&a1, new size_t(size));
        for (size_t i = 0; i < a1.size; ++i)
            a1[i] = uid(mt);

        mes_id<array> a2_id;
        if (checking)
        {
            array& a2 = *new array(size);
            a2_id = add_message(&a2, new size_t(size));
            for (size_t i = 0; i < a1.size; ++i)
                a2[i] = a1[i];
        }
        t = MPI_Wtime();

        create_child_task<quick_task>(a1_id);

        if (checking)
            add_dependence(this_task_id<init_task>(), create_task<check_task>(a1_id.as_const(), a2_id, arg_id<1, double>().as_const()));
        else
            add_dependence(this_task_id<init_task>(), create_task<check_task>(arg_id<1, double>().as_const()));
    }
};

class check_task: public task
{
public:
    void operator()(const array& a1, array& a2, double t)
    {
        double tm1 = MPI_Wtime();
        std::sort(a2.p, a2.p + a2.size);
        for (size_t i = 0; i < a1.size; ++i)
            if (a1.p[i] != a2.p[i])
            {
                std::cout << "wrong\n";
                goto gh;
            }
        std::cout << "correct\n";
        gh:
        std::cout << tm1 - t << std::endl;
    }

    void operator()(double t)
    {
        std::cout << MPI_Wtime() - t << std::endl;
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    size_t sz = 100000;
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            sz = atoll(argv[++i]);
        }
        else if ((strcmp(argv[i], "-l") == 0) || (strcmp(argv[i], "-limit") == 0))
        {
            quick_task::pred = atoll(argv[++i]);
            pred_initialized = true;
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            checking = true;
        }
    }

    parallelizer pz;

    size_t comm_size = pz.get_proc_count();
    if (!pred_initialized)
        quick_task::pred = sz / (comm_size * 3 / 2);

    double time = 0;
    init_task it;

    pz.execution(&it, std::make_tuple(), const_cast<const size_t*>(&sz), &time);
}
