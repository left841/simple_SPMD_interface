#include <vector>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <algorithm>
#include "parallel.h"

using namespace std;
using namespace apl;

class time_cl: public message
{
public:
    double time;

    time_cl(): time(0.0)
    { }

    void send(const sender& se)
    { se.send(&time); }

    void recv(const receiver& re)
    { re.recv(&time); }
};

struct array_size: public message
{
    size_t size;

    array_size(size_t sz = 0): size(sz)
    { }

    void send(const sender& se)
    { se.send(&size); }

    void recv(const receiver& re)
    { re.recv(&size); }
};

class m_array: public message
{
private:
    int* p;
    size_t size_;
    bool res;
public:

    struct part_info: public sendable
    {
        size_t offset, size;

        part_info(size_t off = 0, size_t sz = 0): offset(off), size(sz)
        { }

        void send(const sender& se)
        {
            se.send(&offset);
            se.send(&size);
        }

        void recv(const receiver& re)
        {
            re.recv(&offset);
            re.recv(&size);
        }
    };

    m_array(size_t sz, int* pt = nullptr): size_(sz), p(pt)
    {
        if (p == nullptr)
        {
            p = new int[size_];
            res = true;
        }
        else
            res = false;
    }

    m_array(const m_array& mes, const part_info& pi): size_(pi.size)
    {
        p = mes.p + pi.offset;
        res = false;
    }

    m_array(const part_info& pi): size_(pi.size)
    {
        p = new int[size_];
        res = true;
    }

    m_array(const array_size sz): size_(sz.size)
    {
        p = new int[size_];
        res = true;
    }

    ~m_array()
    {
        if (res)
            delete[] p;
    }

    void include(const m_array& child, const part_info& pi)
    {
        if (child.res)
        {
            int* q = p + pi.offset;
            for (size_t i = 0; i < child.size_; ++i)
                q[i] = child.p[i];
        }
    }

    void send(const sender& se)
    { se.isend(p, size_); }

    void recv(const receiver& re)
    { re.irecv(p, size_); }

    int& operator[](size_t n)
    { return p[n]; }

    const int& operator[](size_t n) const
    { return p[n]; }

    size_t size() const
    { return size_; }
};

class merge_t_all: public task
{
public:

    merge_t_all(const vector<message*> vm, const vector<const message*> cvm): task(vm, cvm)
    { }
    void perform()
    {
        m_array& s1 = dynamic_cast<m_array&>(arg(0));
        m_array& s2 = dynamic_cast<m_array&>(arg(1));

        for (size_t i = 0; i < s1.size(); ++i)
            s2[i] = s1[i];
        merge_it(&s1[0], &s2[0], s1.size() / 2, s1.size());
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

    merge_t(const vector<message*> vm, const vector<const message*> cvm): task(vm, cvm)
    { }

    void perform()
    {
        const m_array& src = dynamic_cast<const m_array&>(const_arg(0));
        m_array& out = dynamic_cast<m_array&>(arg(0));
        size_t h_size = src.size() / 2;
        size_t first = 0, second = h_size;

        for (size_t i = 0; i < out.size(); ++i)
        {
            if ((first >= h_size))
                out[i] = src[second++];
            else if ((second < src.size()) && (src[second] < src[first]))
                out[i] = src[second++];
            else
                out[i] = src[first++];
        }
    }
};

class merge_organizer: public task
{
public:

    static size_t pred;

    merge_organizer(const vector<message*> vm, const vector<const message*> cvm): task(vm, cvm)
    { }

    void perform()
    {
        const m_array& in = dynamic_cast<const m_array&>(const_arg(0));
        const m_array& out = dynamic_cast<const m_array&>(const_arg(1));

        if (in.size() <= pred)
            create_child_task<merge_t_all>({const_arg_id(0), const_arg_id(1)}, {});
        else
        {
            size_t half_size = in.size() / 2;
            local_message_id in1 = create_message_child<m_array>(const_arg_id(0), new m_array::part_info(0, half_size));
            local_message_id in2 = create_message_child<m_array>(const_arg_id(0), new m_array::part_info(half_size, in.size() - half_size));

            local_message_id out1 = create_message_child<m_array>(const_arg_id(1), new m_array::part_info(0, half_size));
            local_message_id out2 = create_message_child<m_array>(const_arg_id(1), new m_array::part_info(half_size, out.size() - half_size));

            local_task_id org1 = create_child_task<merge_organizer>({}, {out1, in1});
            local_task_id org2 = create_child_task<merge_organizer>({}, {out2, in2});
            local_task_id mer = create_child_task<merge_t>({const_arg_id(1)}, {const_arg_id(0)});

            add_dependence(org1, mer);
            add_dependence(org2, mer);
        }
    }
};

size_t merge_organizer::pred = 1000;

class check_task: public task
{
public:
    check_task(const vector<message*>& mes_v, const vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        const time_cl& t = dynamic_cast<const time_cl&>(const_arg(0));
        m_array& a1 = dynamic_cast<m_array&>(arg(0));
        m_array& a2 = dynamic_cast<m_array&>(arg(1));
        double tm1 = MPI_Wtime();
        sort(&a2[0], &a2[0] + a2.size());
        /*double tm2 = MPI_Wtime();
        for (int i = 0; i < a1.get_size(); ++i)
            cout << a1.get_p()[i] << ' ';
        cout << endl;
        for (int i = 0; i < a2.get_size(); ++i)
            cout << a2.get_p()[i] << ' ';
        cout << endl;*/
        for (size_t i = 0; i < a1.size(); ++i)
            if (a1[i] != a2[i])
            {
                cout << "wrong\n";
                goto gh;
            }
        cout << "correct\n";
        gh:
        cout << tm1 - t.time << endl;
        //cout << tm2 - tm1 << endl;
    }
};

class init_task: public task
{
public:

    init_task(const vector<message*>& mes_v, const vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        mt19937 mt(static_cast<int>(time(0)));
        uniform_int_distribution<int> uid(0, 10000);
        time_cl& t = dynamic_cast<time_cl&>(arg(0));
        const array_size& size = dynamic_cast<const array_size&>(const_arg(0));
        m_array& a1 = *new m_array(size);
        m_array& a2 = *new m_array(size);
        m_array& a3 = *new m_array(size);

        for (size_t i = 0; i < a1.size(); ++i)
            a1[i] = a2[i] = a3[i] = uid(mt);
        t.time = MPI_Wtime();

        local_message_id m_id1 = add_message_init(&a1, new array_size(size));
        local_message_id m_id2 = add_message_init(&a2, new array_size(size));
        local_message_id m_id3 = add_message_init(&a3, new array_size(size));

        local_task_id merge = create_task<merge_organizer>({}, {m_id1, m_id3});
        local_task_id check = create_task<check_task>({m_id3, m_id2}, {arg_id(0)});

        add_dependence(merge, check);
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    size_t size = 100000;
    if (argc > 1)
    {
        size = atoll(argv[1]);
        if (argc > 2)
            merge_organizer::pred = atoll(argv[2]);
    }

    parallelizer pz;

    int comm_size = pz.get_proc_count();
    merge_organizer::pred = size / comm_size;

    pz.execution(new init_task({new time_cl()}, {new array_size(size)}));
}
