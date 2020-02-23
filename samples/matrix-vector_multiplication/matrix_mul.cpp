#include <vector>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <algorithm>
#include <iostream>
#include "parallel.h"
using namespace std;
using namespace auto_parallel;

int n = 100, m = 50, layers = 8;

class mymessage: public message
{
public:
    int size;
    int* arr;
    mymessage(int _size, int* _arr): message(), size(_size), arr(_arr)
    { }
    void send(const sender& se)
    { se.isend(arr, size, MPI_INT); }
    void recv(const receiver& re)
    {
        if (arr == nullptr)
            arr = new int[size];
        re.irecv(arr, size, MPI_INT);
    }
};

class matrix_part: public message
{
public:
    int** arr;
    int size, length;
    matrix_part(int _l, int _s): message(), length(_l), size(_s)
    {
        arr = new int*[size];
        arr[0] = nullptr;
    }
    void send(const sender& se)
    { se.isend(arr[0], size * length, MPI_INT); }

    void recv(const receiver& re)
    {
        if (arr[0] == nullptr)
        {
            arr[0] = new int[size * length];
            for (int i = 0; i < size; ++i)
                arr[i] = arr[0] + length * i;
        }
        re.irecv(arr[0], size * length, MPI_INT);
    }
};


class onemessage: public message
{
public:
    int a;
    onemessage(int _a): message(), a(_a)
    { }
    void send(const sender& se)
    { se.isend(&a, 1, MPI_INT); }
    void recv(const receiver& re)
    { re.irecv(&a, 1, MPI_INT); }
};


class mytask: public task
{
public:
    mytask(std::vector<message*>& mes_v, std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform(task_environment& env)
    {
        const matrix_part& mp = (const matrix_part&)get_c(0);
        const mymessage& vb = (const mymessage&)get_c(1);
        mymessage& c = (mymessage&)get_a(0);
        for (int i = 0; i < mp.size; ++i)
        {
            c.arr[i] = 0;
            for (int j = 0; j < mp.length; ++j)
                c.arr[i] += mp.arr[i][j] * vb.arr[j];
        }
    }
};

class out_task: public task
{
public:
    out_task(std::vector<message*>& mes_v, std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform(task_environment& env)
    {
        int* a = ((mymessage*)data[0])->arr;
        int size = ((mymessage*)data[0])->size;
        for(int i = 0; i < layers; i++)
        {
            mymessage& me = *((mymessage*)c_data[i]);
            for (int j = 0; j < me.size; ++j)
                a[j] = me.arr[j];
            a += me.size;
        }
    }
};

class init_task : public task
{
    public:
    init_task(std::vector<message*>& mes_v, std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform(task_environment& env)
    {
        int**& a = ((matrix_part*)data[0])->arr;
        a[0] = new int[size_t(n) * m];
        int* b = a[0];
        int tn = 0;
        for (int i = 0; i < layers; i++)
        {
            int& s = ((matrix_part*)data[i])->size;
            int& l = ((matrix_part*)data[i])->length;
            for (int j = 0; j < s; ++j)
            {
                ((matrix_part*)data[i])->arr[j] = b + j * l;
                for (int k = 0; k < l; ++k)
                   b[j * l + k] = tn++;
            }
            b += s * l;
        }
    }
};

int main(int argc, char** argv)
{
    if (argc > 1)
    {
        n = atoi(argv[1]);
        if (argc > 2)
        {
            m = atoi(argv[2]);
            if (argc > 3)
                layers = atoi(argv[3]);
        }
    }
    int* b, *c;
    b = new int[m];
    c = new int[n];
    int tn = 0;
    tn = m;
    for(int i = 0; i < m; i++)
        b[i] = tn--;
    parallelizer pz(&argc, &argv);
    task_graph gr;
    mymessage* w = new mymessage(m, b);
    mymessage* cw = new mymessage(n, c);
    mytask** t = new mytask*[layers];
    vector<message*> ve;
    vector<const message*> cve;
    vector<message*> vi;
    vector<const message*> cvi;
    ve.push_back(cw);
    int div = n / layers;
    int mod = n % layers;
    for (int i = 0; i < layers; ++i)
    {
        int g = div;
        if (i == layers - 1)
            g += mod;
        matrix_part* p = new matrix_part(m, g);
        mymessage* q = new mymessage(div, new int[g]);
        vector<message*> v;
        vector<const message*> cv;
        cv.push_back(p);
        cv.push_back(w);
        v.push_back(q);
        cve.push_back(q);
        t[i] = new mytask(v,cv);
        vi.push_back(p);
    }
    out_task* te = new out_task(ve,cve);
    init_task * ti = new init_task(vi, cvi);
    for (int i = 0; i < layers; ++i)
    {
        gr.add_dependence(t[i], te);
        gr.add_dependence(ti, t[i]);
    }
    pz.init(gr);
    pz.execution();
    if (pz.get_current_proc() == 0)
    {
        double time = MPI_Wtime();
        cout << time - pz.get_start_time();
    }
}
