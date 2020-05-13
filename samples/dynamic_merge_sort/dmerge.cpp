#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
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

class array: public message
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

    array(const array& mes, const part_info& pi): size_(pi.size)
    {
        p = mes.p + pi.offset;
        res = false;
    }

    array(const part_info& pi): size_(pi.size)
    {
        p = new int[size_];
        res = true;
    }

    array(const array_size sz): size_(sz.size)
    {
        p = new int[size_];
        res = true;
    }

    ~array()
    {
        if (res)
            delete[] p;
    }

    void include(const array& child, const part_info& pi)
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

class merge_all_task: public task
{
public:

    merge_all_task(const std::vector<message*> vm, const std::vector<const message*> cvm): task(vm, cvm)
    { }
    void perform()
    {
        array& s1 = dynamic_cast<array&>(arg(0));
        array& s2 = dynamic_cast<array&>(arg(1));

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

class merge_task: public task
{
public:

    merge_task(const std::vector<message*> vm, const std::vector<const message*> cvm): task(vm, cvm)
    { }

    void perform()
    {
        const array& src = dynamic_cast<const array&>(const_arg(0));
        array& out = dynamic_cast<array&>(arg(0));
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

    merge_organizer(const std::vector<message*> vm, const std::vector<const message*> cvm): task(vm, cvm)
    { }

    void perform()
    {
        const array& in = dynamic_cast<const array&>(const_arg(0));
        const array& out = dynamic_cast<const array&>(const_arg(1));

        if (in.size() <= pred)
            create_child_task<merge_all_task>({const_arg_id(0), const_arg_id(1)}, {});
        else
        {
            size_t half_size = in.size() / 2;
            local_message_id in1 = create_message_child<array>(const_arg_id(0), new array::part_info(0, half_size));
            local_message_id in2 = create_message_child<array>(const_arg_id(0), new array::part_info(half_size, in.size() - half_size));

            local_message_id out1 = create_message_child<array>(const_arg_id(1), new array::part_info(0, half_size));
            local_message_id out2 = create_message_child<array>(const_arg_id(1), new array::part_info(half_size, out.size() - half_size));

            local_task_id org1 = create_child_task<merge_organizer>({}, {out1, in1});
            local_task_id org2 = create_child_task<merge_organizer>({}, {out2, in2});
            local_task_id mer = create_child_task<merge_task>({const_arg_id(1)}, {const_arg_id(0)});

            add_dependence(org1, mer);
            add_dependence(org2, mer);
        }
    }
};

size_t merge_organizer::pred = 1000;

class check_task: public task
{
public:
    static bool checking;

    check_task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        const time_cl& t = dynamic_cast<const time_cl&>(const_arg(0));
        array& a1 = dynamic_cast<array&>(arg(0));
        array& a2 = dynamic_cast<array&>(arg(1));
        double tm1 = MPI_Wtime();
        if (checking)
        {
            std::sort(&a2[0], &a2[0] + a2.size());
            for (size_t i = 0; i < a1.size(); ++i)
                if (a1[i] != a2[i])
                {
                    std::cout << "wrong\n";
                    goto gh;
                }
            std::cout << "correct\n";
        }
        gh:
        std::cout << tm1 - t.time << std::endl;
    }
};

bool check_task::checking = false;

class init_task: public task
{
public:

    init_task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        std::mt19937 mt(static_cast<int>(time(0)));
        std::uniform_int_distribution<int> uid(0, 10000);
        time_cl& t = dynamic_cast<time_cl&>(arg(0));
        const array_size& size = dynamic_cast<const array_size&>(const_arg(0));
        array& a1 = *new array(size);
        array& a2 = *new array(size);
        array& a3 = *new array(size);

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
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            size = atoll(argv[++i]);
        }
        else if ((strcmp(argv[i], "-l") == 0) || (strcmp(argv[i], "-limit") == 0))
        {
            merge_organizer::pred = atoll(argv[++i]);
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            check_task::checking = true;
        }
    }

    parallelizer pz;

    int comm_size = pz.get_proc_count();
    merge_organizer::pred = size / comm_size;

    pz.execution(new init_task({new time_cl()}, {new array_size(size)}));
}
