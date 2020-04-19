#include <vector>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <algorithm>
#include "parallel.h"

using namespace std;
using namespace apl;

class m_array: public message
{
private:
    int* p;
    int size_;
    bool res;
public:

    m_array(int sz, int* pt = nullptr): size_(sz), p(pt)
    {
        if (p == nullptr)
        {
            p = new int[size_];
            res = true;
        }
        else
            res = false;
    }
    ~m_array()
    {
        if (res)
            delete[] p;
    }
    void send(const sender& se)
    { se.isend(p, size_); }

    void recv(const receiver& re)
    { re.irecv(p, size_); }

    int& operator[](size_t n)
    { return p[n]; }

    const int& operator[](size_t n) const
    { return p[n]; }

    int size() const
    { return size_; }

    int* data()
    { return p; }
};

class merge_t: public task
{
public:
    merge_t(): task()
    { }
    merge_t(const vector<message*> vm, const vector<const message*> cvm): task(vm, cvm)
    { }
    void perform()
    {
        const m_array& s1 = dynamic_cast<const m_array&>(const_arg(0));
        const m_array& s2 = dynamic_cast<const m_array&>(const_arg(1));
        m_array& out = dynamic_cast<m_array&>(arg(0));

        int first = 0, second = 0;
        for (int i = 0; i < out.size(); ++i)
        {
            if ((first >= s1.size()))
                out[i] = s2[second++];
            else if ((second < s2.size()) && (s2[second] < s1[first]))
                out[i] = s2[second++];
            else
                out[i] = s1[first++];
        }
    }

    m_array* get_out()
    { return (m_array*)&arg(0); }

    m_array* get_first()
    { return (m_array*)&const_arg(0); }

    m_array* get_second()
    { return (m_array*)&const_arg(1); }

};

class merge_t_all: public task
{
public:
    merge_t_all(): task()
    { }
    merge_t_all(const vector<message*> vm): task(vm)
    { }
    void perform()
    {
        m_array& s1 = dynamic_cast<m_array&>(arg(0));
        m_array& s2 = dynamic_cast<m_array&>(arg(1));
        for (int i = 0; i < s1.size(); ++i)
            s2[i] = s1[i];
        merge_it(&s1[0], &s2[0], s1.size()/2, s1.size());
    }
    void merge_it(int* s, int* out, int size1, int size2)
    {
        if (size2 < 2)
        {
            for (int i = 0; i < size2; ++i)
                out[i] = s[i];
            return;
        }
        merge_it(out, s, size1 / 2, size1);
        merge_it(out + size1, s + size1, (size2 - size1) / 2, size2 - size1);
        int first = 0;
        int second = size1;
        for (int i = 0; i < size2; ++i)
        {
            if ((first == size1))
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
    int layers = 2;
    int size = 100000;
    if (argc > 1)
    {
        size = atoi(argv[1]);
        if (argc > 2)
            layers = atoi(argv[2]);
    }

    int* p1 = new int[size];
    int* p2 = new int[size];
    int* p3 = new int[size];
    mt19937 mt(static_cast<unsigned>(time(0)));
    uniform_int_distribution<int> uid(0, 10000);
    for (int i = 0; i < size; ++i)
        p1[i] = p3[i] = uid(mt);
    
    double true_start_time = MPI_Wtime();
    parallelizer pz;
    task_graph tg;

    int comm_size = pz.get_proc_count();
    {
        int j = 1;
        int i = 0;
        while (j < comm_size)
        {
            j <<= 1;
            ++i;
        }
        layers = i;
    }

    vector<message*> fin;
    vector<task*> v1, v2;
    int g = 1 << layers;
    if (layers != 0)
    {
        if (layers % 2 == 0)
            swap(p1,p2);
        m_array* arr1 = new m_array(size / 2, p2);
        m_array* arr2 = new m_array(size - size / 2, p2 + size / 2);
        m_array* arr_p1 = new m_array(size, p1);
        v2.push_back(new merge_t({arr_p1}, {arr1, arr2}));
        fin.push_back(arr_p1);
        for (int i = 1; i < layers; ++i)
        {
            int q = 1 << i;
            v1.resize(q);
            for (int j = 0; j < q; ++j)
            {
                m_array* me;
                int* ptr;
                if (j%2)
                {
                    me = &((m_array&)((merge_t*)v2[j/2])->const_arg(1));
                    ptr = ((merge_t*)v2[j/2])->get_out()->data() + ((merge_t*)v2[j/2])->get_first()->size();
                }
                else
                {
                    me = ((merge_t*)v2[j/2])->get_first();
                    ptr = ((merge_t*)v2[j/2])->get_out()->data();
                }

                arr1 = new m_array(me->size() / 2, ptr);

                arr2 = new m_array(me->size() - me->size() / 2, ptr + me->size() / 2);
                arr_p1 = me;

                v1[j] = new merge_t({arr_p1}, {arr1, arr2});

                tg.add_dependence(v1[j], v2[j/2]);
            }
            swap(v1, v2);
        }

        for (int i = 0; i < v2.size(); ++i)
        {
            arr1 = new m_array(((merge_t*)v2[i])->get_first()->size(), ((merge_t*)v2[i])->get_out()->data());
            arr2 = ((merge_t*)v2[i])->get_first();
            tg.add_dependence(new merge_t_all({arr1, arr2}), v2[i]);
            arr1 = new m_array(((merge_t*)v2[i])->get_second()->size(), ((merge_t*)v2[i])->get_out()->data()
                + ((merge_t*)v2[i])->get_first()->size());
            arr2 = ((merge_t*)v2[i])->get_second();
            tg.add_dependence(new merge_t_all({arr1, arr2}), v2[i]);
        }
    }
    else
    {
        m_array* arr1 = new m_array(size, p1);
        m_array* arr2 = new m_array(size, p2);
        tg.add_task(new merge_t_all({arr1, arr2}));
        swap(p1, p2);
        fin.push_back(arr1);
        fin.push_back(arr2);
    }

    pz.init(tg);

    pz.execution();

    if (pz.get_current_proc() == parallelizer::main_proc)
    {
        for (message* i: fin)
            i->wait_requests();
        double dt = MPI_Wtime();
        sort(p3, p3 + size);
        double pt = MPI_Wtime();
        bool fl = false;
        for (int i = 0; i < size; ++i)
            if (p1[i] != p3[i])
                fl = true;
        /*for (int i = 0; i < size; ++i)
            cout << p1[i] << ' ';
        cout << '\n';*/
        if (fl)
            cout << "wrong\n";
        else
            cout << "correct\n";
        cout << dt - true_start_time << endl;//parallel_engine::get_start_time();// << '\n' << pt - dt;
        cout.flush();
    }
}
