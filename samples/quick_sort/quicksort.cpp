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
    { se.send(&time, 1, MPI_DOUBLE); }

    void recv(const receiver& re)
    { re.recv(&time, 1, MPI_LONG_LONG); }
};

class arrray: public message
{
public:
    bool created;
    int* p;
    size_t size;

    struct init_info: public init_info_base
    {
        long long size;

        init_info()
        { size = 0; }

        void send(const sender& se)
        { se.send(&size, 1, MPI_LONG_LONG); }

        void recv(const receiver& re)
        { re.recv(&size, 1, MPI_LONG_LONG); }
    };

    struct part_info: public part_info_base
    {
        int offset;
        long long size;

        part_info()
        { offset = size = 0; }

        void send(const sender& se)
        {
            se.send(&offset, 1, MPI_INT);
            se.send(&size, 1, MPI_LONG_LONG);
        }

        void recv(const receiver& re)
        {
            re.recv(&offset, 1, MPI_INT);
            re.recv(&size, 1, MPI_LONG_LONG);
        }
    };

    arrray(init_info* ii): size(ii->size)
    {
        p = new int[ii->size];
        created = true;
    }

    arrray(message* m, part_info* pi): size(pi->size)
    {
        p = ((arrray*)m)->p + pi->offset;
        created = false;
    }

    ~arrray()
    {
        if (created)
            delete[] p;
    }

    void send(const sender& se)
    { se.isend(p, size, MPI_INT); }

    void recv(const receiver& re)
    { re.irecv(p, size, MPI_INT); }
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

    quick_task(vector<message*>& mes_v): task(mes_v)
    { }

    quick_task(vector<message*>& mes_v, vector<const message*>& c_mes_v): task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        int* a = a1.p;
        int sz = a1.size;

        if (sz < pred)
            simple_quicksort(a, sz);
            //sort(a, a + sz);
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
                arrray::init_info* ii1 = new arrray::init_info;
                ii1->size = r + 1;
                arrray::part_info* pi1 = new arrray::part_info;
                pi1->offset = 0;
                pi1->size = r + 1;
                task_environment::task_info* ti1 = new task_environment::task_info;
                ti1->data.push_back(env.create_message<arrray>(ii1, pi1, env.get_arg_id(0)));
                env.create_task<quick_task>(ti1);
            }

            if (sz - (r + 1) > 1)
            {
                arrray::init_info* ii2 = new arrray::init_info;
                ii2->size = sz - (r + 1);
                arrray::part_info* pi2 = new arrray::part_info;
                pi2->offset = r + 1;
                pi2->size = sz - (r + 1);
                task_environment::task_info* ti2 = new task_environment::task_info;
                ti2->data.push_back(env.create_message<arrray>(ii2, pi2, env.get_arg_id(0)));
                env.create_task<quick_task>(ti2);
            }
        }
    }
};

int quick_task::pred = 1000;

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
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        arrray& a2 = dynamic_cast<arrray&>(*data[1]);
        time_cl& t = dynamic_cast<time_cl&>(*data[2]);
        for (int i = 0; i < a1.size; ++i)
            a1.p[i] = a2.p[i] = uid(mt);
        t.time = MPI_Wtime();
        //cout << quick_task::pred << endl;
    }
};

class check_task: public task
{
public:
    check_task(vector<message*>& mes_v, vector<const message*>& c_mes_v) : task(mes_v, c_mes_v)
    { }

    void perform(task_environment& env)
    {
        const time_cl& t = dynamic_cast<const time_cl&>(*c_data[0]);
        arrray& a1 = dynamic_cast<arrray&>(*data[0]);
        arrray& a2 = dynamic_cast<arrray&>(*data[1]);
        double tm1 = MPI_Wtime();
//        sort(a2.p, a2.p + a2.size);
//        double tm2 = MPI_Wtime();
//        for (int i = 0; i < a1.size; ++i)
//            if (a1.p[i] != a2.p[i])
//            {
//                cout << "wrong\n";
//                goto gh;
//            }
//        cout << "correct\n";
        gh:
        cout << tm1 - t.time << '\n';
        //cout << tm2 - tm1;
        cout.flush();
    }
};

int main(int argc, char** argv)
{
    int sz = 100000;
    if (argc > 1)
    {
        sz = atoi(argv[1]);
        if (argc > 2)
            quick_task::pred = atoi(argv[2]);
    }
    message_factory::add<arrray>();
    message_factory::add_part<arrray>();
    task_factory::add<quick_task>();

    parallelizer pz(&argc, &argv);

    int comm_size = pz.get_proc_count();
    quick_task::pred = sz / (3 * comm_size / 2);

    task_graph tg;
    arrray::init_info ii;
    ii.size = sz;
    vector<message*> v;
    vector<const message*> w(1);
    time_cl* p = new time_cl;
    w[0] = p;
    v.push_back(new arrray(&ii));
    quick_task qt(v);
    v.push_back(new arrray(&ii));
    check_task ct(v, w);
    v.push_back(p);
    init_task it(v);
    tg.add_dependence(it, qt);
    tg.add_dependence(qt, ct);
    pz.init(tg);
    pz.execution();
}
