#include <vector>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <algorithm>
#include "parallel.h"

using namespace std;
using namespace auto_parallel;

class time_cl: public message
{
public:
    double time;

    time_cl()
    { time = 0.0; }

    void send(const sender& se)
    { se.send(&time, 1, MPI_DOUBLE); }

    void recv(const receiver& re)
    { re.recv(&time, 1, MPI_LONG_LONG); }
};

class m_array: public message
{
private:
    int* p;
    int size;
    bool res;
public:

    int layer;

    struct init_info: public init_info_base
    {
        int size;
        int layer;

        init_info()
        {
            size = 0;
            layer = 1;
        }

        void send(const sender& se)
        {
            se.send(&size, 1, MPI_INT);
            se.send(&layer, 1, MPI_INT);
        }

        void recv(const receiver& re)
        {
            re.recv(&size, 1, MPI_INT);
            re.recv(&layer, 1, MPI_INT);
        }
    };

    struct part_info: public part_info_base
    {
        int offset, size;

        part_info()
        { offset = size = 0; }

        void send(const sender& se)
        {
            se.send(&offset, 1, MPI_INT);
            se.send(&size, 1, MPI_INT);
        }

        void recv(const receiver& re)
        {
            re.recv(&offset, 1, MPI_INT);
            re.recv(&size, 1, MPI_INT);
        }
    };

    m_array(int sz, int* pt = nullptr) : size(sz), p(pt)
    {
        if (p == nullptr)
        {
            p = new int[size];
            res = true;
        }
        else
            res = false;
        layer = 1;
    }

    m_array(init_info* ii): size(ii->size), layer(ii->layer)
    {
        p = new int[size];
        res = true;
    }

    m_array(message* mes, part_info* pi): size(pi->size)
    {
        p = dynamic_cast<m_array*>(mes)->p + pi->offset;
        layer = dynamic_cast<m_array*>(mes)->layer + 1;
        res = false;
    }

    ~m_array()
    {
        if (res)
            delete[] p;
    }

    void send(const sender& se)
    { se.isend(p, size, MPI_INT); }

    void recv(const receiver& re)
    { re.irecv(p, size, MPI_INT); }

    int* get_p() const
    { return p; }

    int get_size() const
    { return size; }

    int get_layer() const
    { return layer; }
};

class init_task: public task
{
public:
    init_task(vector<message*>& mes_v) : task(mes_v)
    { }

    init_task(vector<message*>& mes_v, vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        mt19937 mt(time(0));
        uniform_int_distribution<int> uid(0, 10000);
        m_array& a1 = dynamic_cast<m_array&>(*data[0]);
        m_array& a2 = dynamic_cast<m_array&>(*data[1]);
        time_cl& t = dynamic_cast<time_cl&>(*data[2]);
        for (int i = 0; i < a1.get_size(); ++i)
            a1.get_p()[i] = a2.get_p()[i] = uid(mt);
        t.time = MPI_Wtime();
    }
};

class merge_t_all: public task
{
public:
    merge_t_all() : task()
    { }
    merge_t_all(vector<message*> vm, vector<const message*> cvm) : task(vm, cvm)
    { }
    void perform(task_environment& env)
    {
        m_array* s1, *s2;
        s1 = (m_array*)data[0];
        s2 = (m_array*)data[1];
        for (int i = 0; i < s1->get_size(); ++i)
            s2->get_p()[i] = s1->get_p()[i];
        merge_it(s1->get_p(), s2->get_p(), s1->get_size() / 2, s1->get_size());
        for (int i = 0; i < s1->get_size(); ++i)
            s1->get_p()[i] = s2->get_p()[i];
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

class merge_t: public task
{
public:

    static int max_layer;

    merge_t() : task()
    { }
    merge_t(vector<message*> vm, vector<const message*> cvm) : task(vm, cvm)
    { }
    void perform(task_environment& env)
    {
        const m_array& src = dynamic_cast<const m_array&>(*c_data[0]);
        m_array& out = dynamic_cast<m_array&>(*data[0]);
        int* p_out = out.get_p();
        int h_size = src.get_size() / 2;
        int first = 0, second = h_size;

        for (int i = 0; i < src.get_size(); ++i)
            cout << src.get_p()[i] << ' ';
        cout << endl;

        for (int i = 0; i < out.get_size(); ++i)
        {
            if ((first >= h_size))
            {
                p_out[i] = src.get_p()[second++];
                cout << "0 ";
            }
            else if ((second < src.get_size()) && (src.get_p()[second] < src.get_p()[first]))
            {
                p_out[i] = src.get_p()[second++];
                cout << "1 ";
            }
            else
            {
                p_out[i] = src.get_p()[first++];
                cout << "2 ";
            }
        }
        cout << endl;

        cout << "waltype" << endl;
        for (int i = 0; i < out.get_size(); ++i)
            cout << out.get_p()[i] << ' ';
        cout << endl;
        cout << src.layer << ' ' << max_layer << endl;

        m_array::init_info* iis1 = new m_array::init_info, *iis2 = new m_array::init_info;
        m_array::init_info* iio1 = new m_array::init_info, *iio2 = new m_array::init_info;
        iis1->layer = iis2->layer = src.layer + 1;
        iio1->layer = iio2->layer = out.layer + 1;
        iis1->size = iio1->size = h_size;
        iis2->size = iio2->size = src.get_size() - h_size;
        m_array::part_info* pis1 = new m_array::part_info, *pis2 = new m_array::part_info;
        m_array::part_info* pio1 = new m_array::part_info, *pio2 = new m_array::part_info;
        pis1->size = pio1->size = h_size;
        pis2->size = pio2->size = src.get_size() - h_size;
        pis1->offset = pio1->offset = 0;
        pis2->offset = pio2->offset = h_size;
        task_environment::mes_id ids1, ids2, ido1, ido2;
        ids1 = env.create_message<m_array>(iis1, pis1, env.get_c_arg_id(0));
        ids2 = env.create_message<m_array>(iis2, pis2, env.get_c_arg_id(0));
        ido1 = env.create_message<m_array>(iio1, pio1, env.get_arg_id(0));
        ido2 = env.create_message<m_array>(iio2, pio2, env.get_arg_id(0));

        cout << "chifir" << endl;
        cout << src.layer << ' ' << max_layer << endl;
        if (src.layer >= max_layer)
        {
            task_environment::task_info* ti1 = new task_environment::task_info;
            ti1->data.push_back(ido1);
            ti1->data.push_back(ids1);
            env.create_task<merge_t_all>(ti1);
            task_environment::task_info* ti2 = new task_environment::task_info;
            ti2->data.push_back(ido2);
            ti2->data.push_back(ids2);
            env.create_task<merge_t_all>(ti2);
        }
        else
        {
            task_environment::task_info* ti1 = new task_environment::task_info;
            ti1->data.push_back(ids1);
            ti1->c_data.push_back(ido1);
            env.create_task<merge_t>(ti1);
            task_environment::task_info* ti2 = new task_environment::task_info;
            ti2->data.push_back(ids2);
            ti2->c_data.push_back(ido2);
            env.create_task<merge_t>(ti2);
        }
    }

    m_array* get_out()
    { return (m_array*)data[0]; }

    m_array* get_first()
    { return (m_array*)c_data[0]; }

    m_array* get_second()
    { return (m_array*)c_data[1]; }

};

int merge_t::max_layer;

class check_task: public task
{
public:
    check_task(vector<message*>& mes_v, vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        cout << "lul" << endl;
        const time_cl& t = dynamic_cast<const time_cl&>(*c_data[0]);
        m_array& a1 = dynamic_cast<m_array&>(*data[0]);
        m_array& a2 = dynamic_cast<m_array&>(*data[1]);
        double tm1 = MPI_Wtime();
        sort(a2.get_p(), a2.get_p() + a2.get_size());
        double tm2 = MPI_Wtime();
        for (int i = 0; i < a1.get_size(); ++i)
            cout << a1.get_p()[i] << ' ';
        cout << endl;
        for (int i = 0; i < a2.get_size(); ++i)
            cout << a2.get_p()[i] << ' ';
        cout << endl;
        for (int i = 0; i < a1.get_size(); ++i)
            if (a1.get_p()[i] != a2.get_p()[i])
            {
                cout << "wrong\n";
                goto gh;
            }
        cout << "correct\n";
    gh:
        cout << tm1 - t.time << '\n';
        cout << tm2 - tm1;
        cout.flush();
    }
};

int main(int argc, char** argv)
{
    int layers = 2;
    int size = 1000;
    if (argc > 1)
    {
        layers = atoi(argv[1]);
        if (argc > 2)
            size = atoi(argv[2]);
    }
    merge_t::max_layer = layers;

    message_factory::add<m_array>();
    message_factory::add_part<m_array>();
    task_factory::add<merge_t>();
    task_factory::add<merge_t_all>();

    parallelizer pz;
    task_graph tg;
    m_array::init_info ii;
    ii.size = size;

    message* m1 = new m_array(&ii);
    message* m2 = new m_array(&ii);
    message* m3 = new m_array(&ii);
    time_cl* p = new time_cl;
    vector<message*> v;
    v.push_back(m1);
    v.push_back(m2);
    v.push_back(p);
    init_task it(v);
    v.clear();
    v.push_back(m3);
    v.push_back(m2);
    vector<const message*> w(1);
    w[0] = p;
    check_task ct(v, w);
    v.clear();
    w.clear();
    if (layers > 0)
    {
        v.push_back(m3);
        w.push_back(m1);
        merge_t* qt = new merge_t(v, w);
        tg.add_dependence(&it, qt);
        tg.add_dependence(qt, &ct);
    }
    else
    {
        v.push_back(m1);
        v.push_back(m3);
        merge_t_all* qt = new merge_t_all(v, vector<const message*>());
        tg.add_dependence(&it, qt);
        tg.add_dependence(qt, &ct);
    }
    pz.init(tg);
    pz.execution();
}
