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
    { se.send(&time, 1); }

    void recv(const receiver& re)
    { re.recv(&time, 1); }
};

class m_array: public message
{
private:
    int* p;
    int size;
    bool res;
public:

    struct init_info: public init_info_base
    {
        int size;

        init_info(int sz = 0): size(sz)
        { }

        void send(const sender& se)
        {
            se.send(&size, 1);
        }

        void recv(const receiver& re)
        {
            re.recv(&size, 1);
        }
    };

    struct part_info: public part_info_base
    {
        int offset, size;

        part_info(int off = 0, int sz = 0): offset(off), size(sz)
        { }

        void send(const sender& se)
        {
            se.send(&offset, 1);
            se.send(&size, 1);
        }

        void recv(const receiver& re)
        {
            re.recv(&offset, 1);
            re.recv(&size, 1);
        }
    };

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

    m_array(init_info* ii): size(ii->size)
    {
        p = new int[size];
        res = true;
    }

    m_array(message* mes, part_info* pi): size(pi->size)
    {
        p = dynamic_cast<m_array*>(mes)->p + pi->offset;
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

class init_task: public task
{
public:
    init_task(vector<message*>& mes_v) : task(mes_v)
    { }

    init_task(vector<message*>& mes_v, vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        mt19937 mt(static_cast<int>(time(0)));
        uniform_int_distribution<int> uid(0, 10000);
        m_array& a1 = dynamic_cast<m_array&>(*data[0]);
        m_array& a2 = dynamic_cast<m_array&>(*data[1]);
        m_array& a3 = dynamic_cast<m_array&>(*data[2]);
        time_cl& t = dynamic_cast<time_cl&>(*data[3]);
        for (int i = 0; i < a1.get_size(); ++i)
            a1.get_p()[i] = a2.get_p()[i] = a3.get_p()[i] = uid(mt);
        t.time = MPI_Wtime();
    }
};

class merge_t_all: public task
{
public:
    merge_t_all(): task()
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
            if ((first >= size1))
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

    merge_t(): task()
    { }
    merge_t(vector<message*> vm, vector<const message*> cvm): task(vm, cvm)
    { }
    void perform(task_environment& env)
    {
        const m_array& src = dynamic_cast<const m_array&>(*c_data[0]);
        m_array& out = dynamic_cast<m_array&>(*data[0]);
        int* p_out = out.get_p();
        int h_size = src.get_size() / 2;
        int first = 0, second = h_size;

        for (int i = 0; i < out.get_size(); ++i)
        {
            if ((first >= h_size))
                p_out[i] = src.get_p()[second++];
            else if ((second < src.get_size()) && (src.get_p()[second] < src.get_p()[first]))
                p_out[i] = src.get_p()[second++];
            else
                p_out[i] = src.get_p()[first++];
        }
    }

    m_array* get_out()
    { return (m_array*)data[0]; }

    m_array* get_first()
    { return (m_array*)c_data[0]; }

    m_array* get_second()
    { return (m_array*)c_data[1]; }

};

class merge_organizer: public task
{
public:

    static size_t pred;

    merge_organizer(vector<message*> vm, vector<const message*> cvm): task(vm, cvm)
    { }

    void perform(task_environment& env)
    {
        const m_array& in = dynamic_cast<const m_array&>(get_c(0));
        const m_array& out = dynamic_cast<const m_array&>(get_c(1));

        if (in.get_size() < pred)
            env.create_child_task<merge_t_all>({env.get_c_arg_id(0), env.get_c_arg_id(1)}, {});
        else
        {
            size_t half_size = in.get_size() / 2;
            local_message_id in1 = env.create_message<m_array>(new m_array::init_info(half_size), new m_array::part_info(0, half_size), env.get_c_arg_id(0));
            local_message_id in2 = env.create_message<m_array>(new m_array::init_info(in.get_size() - half_size), new m_array::part_info(half_size, in.get_size() - half_size), env.get_c_arg_id(0));

            local_message_id out1 = env.create_message<m_array>(new m_array::init_info(half_size), new m_array::part_info(0, half_size), env.get_c_arg_id(1));
            local_message_id out2 = env.create_message<m_array>(new m_array::init_info(out.get_size() - half_size), new m_array::part_info(half_size, out.get_size() - half_size), env.get_c_arg_id(1));

            local_task_id org1 = env.create_child_task<merge_organizer>({}, {out1, in1});
            local_task_id org2 = env.create_child_task<merge_organizer>({}, {out2, in2});
            local_task_id mer = env.create_child_task<merge_t>({env.get_c_arg_id(1)}, {env.get_c_arg_id(0)});

            env.add_dependence(org1, mer);
            env.add_dependence(org2, mer);
        }
    }
};

size_t merge_organizer::pred = 1000;

class check_task: public task
{
public:
    check_task(vector<message*>& mes_v, vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        const time_cl& t = dynamic_cast<const time_cl&>(*c_data[0]);
        m_array& a1 = dynamic_cast<m_array&>(*data[0]);
        m_array& a2 = dynamic_cast<m_array&>(*data[1]);
        double tm1 = MPI_Wtime();
    //    sort(a2.get_p(), a2.get_p() + a2.get_size());
    //    double tm2 = MPI_Wtime();
    //    /*for (int i = 0; i < a1.get_size(); ++i)
    //        cout << a1.get_p()[i] << ' ';
    //    cout << endl;
    //    for (int i = 0; i < a2.get_size(); ++i)
    //        cout << a2.get_p()[i] << ' ';
    //    cout << endl;*/
    //    for (int i = 0; i < a1.get_size(); ++i)
    //        if (a1.get_p()[i] != a2.get_p()[i])
    //        {
    //            cout << "wrong\n";
    //            goto gh;
    //        }
    //    cout << "correct\n";
    //gh:
        cout << tm1 - t.time << '\n';
        //cout << tm2 - tm1;
        cout.flush();
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    int size = 100000;
    if (argc > 1)
    {
        size = atoi(argv[1]);
        if (argc > 2)
            merge_organizer::pred = atoll(argv[2]);
    }

    message_factory::add<m_array>();
    message_factory::add_part<m_array>();
    task_factory::add<merge_t>();
    task_factory::add<merge_t_all>();
    task_factory::add<merge_organizer>();

    parallelizer pz;
    task_graph tg;
    m_array::init_info ii;
    ii.size = size;

    int comm_size = pz.get_proc_count();
    merge_organizer::pred = size / comm_size;

    message* m1 = new m_array(&ii);
    message* m2 = new m_array(&ii);
    message* m3 = new m_array(&ii);
    time_cl* p = new time_cl;
    vector<message*> v;
    v.push_back(m1);
    v.push_back(m2);
    v.push_back(m3);
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
    w.push_back(m1);
    w.push_back(m3);
    merge_organizer* qt = new merge_organizer(v, w);
    tg.add_dependence(&it, qt);
    tg.add_dependence(qt, &ct);
    pz.init(tg);
    pz.execution();
}
