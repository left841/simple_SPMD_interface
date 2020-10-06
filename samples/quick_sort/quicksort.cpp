#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
#include "array_wrapper.h"
using namespace apl;

class quick_task: public task
{
public:
    static size_t pred;

    void operator()(int* a, size_t sz)
    {
        if (sz < 2)
            return;

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

        if (sz <= pred)
        {
            (*this)(a, r + 1);
            (*this)(a + r + 1, sz - r - 1);
        }
        else
        {
            if (r + 1 > 1)
            {
                mes_id<int*> p1 = create_message_child<array_wrapper<int>>(arg_id<0, array_wrapper<int>>(), new size_t(r + 1), new size_t(0));
                auto s1 = add_message(new size_t(r + 1));
                create_child_task<quick_task>(p1, s1.as_const());
            }

            if (sz - (r + 1) > 1)
            {
                mes_id<int*> p2 = create_message_child<array_wrapper<int>>(arg_id<0, array_wrapper<int>>(), new size_t(sz - r - 1), new size_t(r + 1));
                auto s2 = add_message(new size_t(sz - r - 1));
                create_child_task<quick_task>(p2, s2.as_const());
            }
        }
    }
};

size_t quick_task::pred = 1000;

class check_task;

void dummy_f(int yy)
{
    std::cout << "i am dummy_f" << yy << std::endl;
}

class init_task: public task
{
public:

    void operator()(size_t size, double& t)
    {
        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(0, 10000);
        int* a1 = new int[size];
        int* a2 = new int[size];
        for (size_t i = 0; i < size; ++i)
            a1[i] = a2[i] = uid(mt);
        t = MPI_Wtime();

        mes_id<void(int)> qwe = create_message<void(int), dummy_f>();

        mes_id<int*> a1_id = add_message(make_array(a1, size), new size_t(size));
        mes_id<int*> a2_id = add_message(make_array(a2, size), new size_t(size));

        create_child_task<quick_task>(a1_id, arg_id<0, const size_t>());
        local_task_id end = create_task<check_task>(a1_id.as_const(), a2_id, arg_id<0, const size_t>(), arg_id<1, double>().as_const());
        add_dependence(this_task_id<init_task>(), end);
    }
};

class check_task: public task
{
public:
    static bool checking;

    void operator()(const int* a1, int* a2, size_t size, double t)
    {
        double tm1 = MPI_Wtime();
        if (checking)
        {
            std::sort(a2, a2 + size);
            for (size_t i = 0; i < size; ++i)
                if (a1[i] != a2[i])
                {
                    std::cout << "wrong\n";
                    goto gh;
                }
            std::cout << "correct\n";
        }
        gh:
        std::cout << tm1 - t << std::endl;
    }
};

bool check_task::checking = false;
bool pred_initialized = false;

int main(int argc, char** argv)
{

    parallel_engine pe(&argc, &argv);
    size_t sz = 100000;
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            sz = atoll(argv[++i]);
        }
        else if ((strcmp(argv[i], "-l") == 0) || (strcmp(argv[i], "-limit") == 0))
        {
            quick_task::pred = atoll(argv[++i]);
            pred_initialized = true;
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            check_task::checking = true;
        }
    }

    parallelizer pz;

    int comm_size = pz.get_proc_count();
    if (!pred_initialized)
        quick_task::pred = sz / (3 * comm_size / 2);

    init_task it;
    pz.execution(&it, std::make_tuple(), const_cast<const size_t*>(new size_t(sz)), new double(0));
}
