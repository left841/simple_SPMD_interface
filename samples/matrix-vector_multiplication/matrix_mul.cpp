#include <vector>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <algorithm>
#include <iostream>
#include "parallel.h"
using namespace std;
using namespace apl;

size_t n = 100, m = 50, layers = 8;

class mymessage: public message
{
public:
    size_t size;
    int* arr;
    mymessage(int _size, int* _arr): message(), size(_size), arr(_arr)
    { }
    void send(const sender& se)
    { se.isend(arr, size); }
    void recv(const receiver& re)
    {
        if (arr == nullptr)
            arr = new int[size];
        re.irecv(arr, size);
    }
};

class matrix_part: public message
{
public:
    int** arr;
    size_t size, length;
    matrix_part(int _l, int _s): message(), length(_l), size(_s)
    {
        arr = new int*[size];
        arr[0] = nullptr;
    }
    void send(const sender& se)
    { se.isend(arr[0], size * length); }

    void recv(const receiver& re)
    {
        if (arr[0] == nullptr)
        {
            arr[0] = new int[size * length];
            for (int i = 0; i < size; ++i)
                arr[i] = arr[0] + length * i;
        }
        re.irecv(arr[0], size * length);
    }
};


class onemessage: public message
{
public:
    int a;
    onemessage(int _a): message(), a(_a)
    { }
    void send(const sender& se)
    { se.isend(&a); }
    void recv(const receiver& re)
    { re.irecv(&a); }
};


class mytask: public task
{
public:
    mytask(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        const matrix_part& mp = (const matrix_part&)const_arg(0);
        const mymessage& vb = (const mymessage&)const_arg(1);
        mymessage& c = (mymessage&)arg(0);
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
    out_task(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        int* a = ((mymessage&)arg(0)).arr;
        int size = ((mymessage&)arg(0)).size;
        for(int i = 0; i < layers; i++)
        {
            const mymessage& me = ((const mymessage&)const_arg(i));
            for (int j = 0; j < me.size; ++j)
                a[j] = me.arr[j];
            a += me.size;
        }
    }
};

class init_task : public task
{
    public:
    init_task(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        int**& a = ((matrix_part&)arg(0)).arr;
        a[0] = new int[size_t(n) * m];
        int* b = a[0];
        int tn = 0;
        for (size_t i = 0; i < layers; i++)
        {
            size_t& s = ((matrix_part&)arg(i)).size;
            size_t& l = ((matrix_part&)arg(i)).length;
            for (size_t j = 0; j < s; ++j)
            {
                ((matrix_part&)arg(i)).arr[j] = b + j * l;
                for (size_t k = 0; k < l; ++k)
                   b[j * l + k] = tn++;
            }
            b += s * l;
        }
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    if (argc > 1)
    {
        n = atoll(argv[1]);
        if (argc > 2)
        {
            m = atoll(argv[2]);
            if (argc > 3)
                layers = atoll(argv[3]);
        }
    }
    int* b, *c;
    b = new int[m];
    c = new int[n];
    int tn = 0;
    tn = m;
    for(int i = 0; i < m; i++)
        b[i] = tn--;
    parallelizer pz;
    task_graph gr;
    mymessage* w = new mymessage(m, b);
    mymessage* cw = new mymessage(n, c);
    mytask** t = new mytask*[layers];
    vector<const message*> cve;
    vector<message*> vi;
    int div = n / layers;
    int mod = n % layers;
    for (int i = 0; i < layers; ++i)
    {
        int g = div;
        if (i == layers - 1)
            g += mod;
        matrix_part* p = new matrix_part(m, g);
        mymessage* q = new mymessage(div, new int[g]);
        cve.push_back(q);
        t[i] = new mytask({q}, {p, w});
        vi.push_back(p);
    }
    out_task* te = new out_task({cw}, cve);
    init_task * ti = new init_task(vi, {});
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
        cout << time - parallel_engine::get_start_time();
    }
}
