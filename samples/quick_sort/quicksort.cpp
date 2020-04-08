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

class arrray: public message
{
public:
    bool created;
    int* p;
    size_t size;

    struct part_info: public sendable
    {
        size_t offset;
        size_t size;

        part_info(): offset(0), size(0)
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

    arrray(size_t sz): size(sz)
    {
        p = new int[size];
        created = true;
    }

    arrray(const arrray& m, const part_info& pi): size(pi.size)
    {
        p = m.p + pi.offset;
        created = false;
    }

    arrray(const part_info& pi) : size(pi.size)
    {
        p = new int[size];
        created = true;
    }

    void include(const arrray& child, const part_info& pi)
    {
        if (child.created)
        {
            int* q = p + pi.offset;
            for (int i = 0; i < child.size; ++i)
                q[i] = child.p[i];
        }
    }

    ~arrray()
    {
        if (created)
            delete[] p;
    }

    void send(const sender& se)
    { se.isend(p, static_cast<int>(size)); }

    void recv(const receiver& re)
    { re.irecv(p, static_cast<int>(size)); }
};

class quick_task: public task
{
private:

    void simple_quicksort(int* a, int size)
    {
        int bel;
        int mi = min(min(a[0], a[size - 1]), a[size / 2]);
        int ma = min(min(a[0], a[size - 1]), a[size / 2]);

        if (size < 2)
            return;

        if ((a[0] > mi) && (a[0] < ma))
            bel = a[0];
        else if ((a[size - 1] > mi) && (a[size - 1] < ma))
            bel = a[size - 1];
        else
            bel = a[size / 2];

        int l = 0, r = size - 1;
        while (l <= r)
        {
            while (a[l] < bel)
                ++l;
            while (a[r] > bel)
                --r;
            if (l <= r)
                swap(a[l++], a[r--]);
        }

        simple_quicksort(a, r + 1);
        simple_quicksort(a + r + 1, size - (r + 1));
    }

public:
    static int pred;

    quick_task(const vector<message*>& mes_v): task(mes_v)
    { }

    quick_task(const vector<message*>& mes_v, const vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        int* a = a1.p;
        int sz = static_cast<int>(a1.size);

        if (sz < pred)
            simple_quicksort(a, sz);
        else
        {
            int bel;
            int mi = min(min(a[0], a[sz - 1]), a[sz / 2]);
            int ma = min(min(a[0], a[sz - 1]), a[sz / 2]);
            if ((a[0] > mi) && (a[0] < ma))
                bel = a[0];
            else if ((a[sz - 1] > mi) && (a[sz - 1] < ma))
                bel = a[sz - 1];
            else
                bel = a[sz / 2];

            int l = 0, r = sz - 1;
            while (l <= r)
            {
                while (a[l] < bel)
                    ++l;
                while (a[r] > bel)
                    --r;
                if (l <= r)
                    swap(a[l++], a[r--]);
            }

            if (r + 1 > 1)
            {
                arrray::part_info* pi1 = new arrray::part_info;
                pi1->offset = 0;
                pi1->size = static_cast<size_t>(r) + 1;
                env.create_child_task<quick_task>({env.create_message_child<arrray, arrray, arrray::part_info>(env.get_arg_id(0), pi1)}, {});
            }

            if (sz - (r + 1) > 1)
            {
                arrray::part_info* pi2 = new arrray::part_info;
                pi2->offset = r + 1;
                pi2->size = static_cast<size_t>(sz) - (static_cast<size_t>(r) + 1);
                env.create_child_task<quick_task>({env.create_message_child<arrray, arrray, arrray::part_info>(env.get_arg_id(0), pi2)}, {});
            }
        }
    }
};

int quick_task::pred = 1000;

class init_task: public task
{
public:
    init_task(const vector<message*>& mes_v) : task(mes_v)
    { }

    init_task(const vector<message*>& mes_v, const vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        mt19937 mt(static_cast<unsigned>(time(0)));
        uniform_int_distribution<int> uid(0, 10000);
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        arrray& a2 = dynamic_cast<arrray&>(*data[1]);
        time_cl& t = dynamic_cast<time_cl&>(*data[2]);
        for (int i = 0; i < a1.size; ++i)
            a1.p[i] = a2.p[i] = uid(mt);
        t.time = MPI_Wtime();
    }
};

class check_task: public task
{
public:
    check_task(const vector<message*>& mes_v, const vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        const time_cl& t = dynamic_cast<const time_cl&>(*c_data[0]);
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        arrray& a2 = dynamic_cast<arrray&>(*data[1]);
        double tm1 = MPI_Wtime();
        //sort(a2.p, a2.p + a2.size);
        //double tm2 = MPI_Wtime();
        //for (int i = 0; i < a1.size; ++i)
        //    if (a1.p[i] != a2.p[i])
        //    {
        //        cout << "wrong\n";
        //        goto gh;
        //    }
        //cout << "correct\n";
        //gh:
        cout << tm1 - t.time << '\n';
        //cout << tm2 - tm1;
        cout.flush();
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);
    int sz = 100000;
    if (argc > 1)
    {
        sz = atoi(argv[1]);
        if (argc > 2)
            quick_task::pred = atoi(argv[2]);
    }

    parallelizer pz;

    int comm_size = pz.get_proc_count();
    quick_task::pred = sz / (3 * comm_size / 2);

    task_graph tg;
    time_cl* p = new time_cl;
    arrray* arr1 = new arrray(sz);
    arrray* arr2 = new arrray(sz);
    quick_task qt({arr1});
    check_task ct({arr1, arr2}, {p});
    init_task it({arr1, arr2, p});
    tg.add_dependence(it, qt);
    tg.add_dependence(qt, ct);
    pz.init(tg);
    pz.execution();
}
