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
    int size;
    bool res;
public:

    m_array(int sz, int* pt = nullptr): size(sz), p(pt)
    {
        if (p == nullptr)
        {
            p = new int[size];
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
    { se.isend(p, size); }

    void recv(const receiver& re)
    { re.irecv(p, size); }

    int* get_p() const
    { return p; }

    int get_size() const
    { return size; }
};

class merge_t: public task
{
public:
    merge_t(): task()
    { }
    merge_t(const vector<message*> vm, const vector<const message*> cvm): task(vm, cvm)
    { }
    void perform(task_environment& env)
    {
        const m_array* s1, *s2;
        m_array* out;
        s1 = (const m_array*)c_data[0];
        s2 = (const m_array*)c_data[1];
        out = (m_array*)data[0];

        int first = 0, second = 0;
        int* p_out = out->get_p();
        for (int i = 0; i < out->get_size(); ++i)
        {
            if ((first >= s1->get_size()))
                p_out[i] = s2->get_p()[second++];
            else if ((second < s2->get_size()) && (s2->get_p()[second] < s1->get_p()[first]))
                p_out[i] = s2->get_p()[second++];
            else
                p_out[i] = s1->get_p()[first++];
        }
    }

    m_array* get_out()
    { return (m_array*)data[0]; }

    m_array* get_first()
    { return (m_array*)c_data[0]; }

    m_array* get_second()
    { return (m_array*)c_data[1]; }

};

class merge_t_all: public task
{
public:
    merge_t_all(): task()
    { }
    merge_t_all(const vector<message*> vm): task(vm)
    { }
    void perform(task_environment& env)
    {
        m_array* s1, *s2;
        s1 = (m_array*)data[0];
        s2 = (m_array*)data[1];
        for (int i = 0; i < s1->get_size(); ++i)
            s2->get_p()[i] = s1->get_p()[i];
        merge_it(s1->get_p(), s2->get_p(), s1->get_size()/2, s1->get_size());
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
                    me = ((merge_t*)v2[j/2])->get_second();
                    ptr = ((merge_t*)v2[j/2])->get_out()->get_p() + ((merge_t*)v2[j/2])->get_first()->get_size();
                }
                else
                {
                    me = ((merge_t*)v2[j/2])->get_first();
                    ptr = ((merge_t*)v2[j/2])->get_out()->get_p();
                }

                arr1 = new m_array(me->get_size() / 2, ptr);

                arr2 = new m_array(me->get_size() - me->get_size() / 2, ptr + me->get_size() / 2);
                arr_p1 = me;

                v1[j] = new merge_t({arr_p1}, {arr1, arr2});

                tg.add_dependence(v1[j], v2[j/2]);
            }
            swap(v1, v2);
        }

        for (int i = 0; i < v2.size(); ++i)
        {
            arr1 = new m_array(((merge_t*)v2[i])->get_first()->get_size(), ((merge_t*)v2[i])->get_out()->get_p());
            arr2 = ((merge_t*)v2[i])->get_first();
            tg.add_dependence(new merge_t_all({arr1, arr2}), v2[i]);
            arr1 = new m_array(((merge_t*)v2[i])->get_second()->get_size(), ((merge_t*)v2[i])->get_out()->get_p()
                + ((merge_t*)v2[i])->get_first()->get_size());
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
        //sort(p3, p3 + size);
        //double pt = MPI_Wtime();
        //bool fl = false;
        //for (int i = 0; i < size; ++i)
        //    if (p1[i] != p3[i])
        //        fl = true;
        ///*for (int i = 0; i < size; ++i)
        //    cout << p1[i] << ' ';
        //cout << '\n';*/
        //if (fl)
        //    cout << "wrong\n";
        //else
        //    cout << "correct\n";
        cout << dt - true_start_time << endl;//parallel_engine::get_start_time();// << '\n' << pt - dt;
        cout.flush();
    }
}
