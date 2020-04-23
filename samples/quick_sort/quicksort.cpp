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

struct init_size: public message
{
    size_t size;

    init_size(size_t sz = 0): size(sz)
    { }

    operator size_t()
    { return size; }

    void send(const sender& se)
    { se.send(&size); }

    void recv(const receiver& re)
    { re.recv(&size); }
};

struct part_info: public sendable
{
    size_t offset;
    size_t size;

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

class array: public message
{
public:
    bool created;
    int* p;
    size_t size;

    array(const init_size& sz): size(sz.size)
    {
        p = new int[size];
        created = true;
    }

    array(const array& m, const part_info& pi): size(pi.size)
    {
        p = m.p + pi.offset;
        created = false;
    }

    array(const part_info& pi): size(pi.size)
    {
        p = new int[size];
        created = true;
    }

    void include(const array& child, const part_info& pi)
    {
        if (child.created)
        {
            int* q = p + pi.offset;
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

    void send(const sender& se)
    { se.isend(p, static_cast<int>(size)); }

    void recv(const receiver& re)
    { re.irecv(p, static_cast<int>(size)); }
};

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
    static int pred;

    quick_task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        array& a = dynamic_cast<array&>(arg(0));
        size_t sz = static_cast<int>(a.size);

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
                local_message_id p1 = create_message_child<array>(arg_id(0), new part_info(0, r + 1));
                create_child_task<quick_task>({p1}, {});
            }

            if (sz - (r + 1) > 1)
            {
                local_message_id p2 = create_message_child<array>(arg_id(0), new part_info(r + 1, sz - r - 1));
                create_child_task<quick_task>({p2}, {});
            }
        }
    }
};

int quick_task::pred = 1000;

class check_task;

class init_task: public task
{
public:

    init_task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(0, 10000);
        const init_size& size = dynamic_cast<const init_size &>(const_arg(0));
        array& a1 = *new array(size);
        array& a2 = *new array(size);
        time_cl& t = dynamic_cast<time_cl&>(arg(0));
        for (int i = 0; i < a1.size; ++i)
            a1[i] = a2[i] = uid(mt);
        t.time = MPI_Wtime();

        local_message_id a1_id = add_message_init(&a1, new init_size(size));
        local_message_id a2_id = add_message_init(&a2, new init_size(size));

        create_child_task<quick_task>({a1_id}, {});
        add_dependence(this_task_id(), create_task<check_task>({a2_id}, {a1_id, arg_id(0)}));
    }
};

class check_task: public task
{
public:
    check_task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform()
    {
        const time_cl& t = dynamic_cast<const time_cl&>(const_arg(1));
        const array& a1 = dynamic_cast<const array&>(const_arg(0));
        array& a2 = dynamic_cast<array&>(arg(0));
        double tm1 = MPI_Wtime();
        std::sort(a2.p, a2.p + a2.size);
        double tm2 = MPI_Wtime();
        for (size_t i = 0; i < a1.size; ++i)
            if (a1.p[i] != a2.p[i])
            {
                std::cout << "wrong\n";
                goto gh;
            }
        std::cout << "correct\n";
        gh:
        std::cout << tm1 - t.time << std::endl;
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    size_t sz = 100000;
    if (argc > 1)
    {
        sz = atoll(argv[1]);
        if (argc > 2)
            quick_task::pred = atoll(argv[2]);
    }

    parallelizer pz;

    int comm_size = pz.get_proc_count();
    quick_task::pred = sz / (3 * comm_size / 2);

    time_cl p;
    init_size is(sz);
    init_task it({&p}, {&is});
    pz.execution(&it);
}
