#include <cstring>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

class array: public message
{
private:
    int* p;
    size_t size_;
    bool res;
public:

    array(const array& mes, const size_t& sz, const size_t& offset): size_(sz)
    {
        p = mes.p + offset;
        res = false;
    }

    array(const size_t& sz, const size_t& offset): size_(sz)
    {
        p = new int[size_];
        res = true;
    }

    array(const size_t sz): size_(sz)
    {
        p = new int[size_];
        res = true;
    }

    ~array()
    {
        if (res)
            delete[] p;
    }

    void include(const array& child, const size_t& sz, const size_t& offset)
    {
        if (child.res)
        {
            int* q = p + offset;
            for (size_t i = 0; i < child.size_; ++i)
                q[i] = child.p[i];
        }
    }

    void send(const sender& se) const
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

    merge_all_task(): task()
    { }
    void operator()(array& s1, array& s2)
    {
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

    merge_task(): task()
    { }

    void operator()(const array& src, array& out)
    {
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

    size_t depth;

    merge_organizer(size_t d): task(), depth(d)
    { }

    void operator()(const array& in, const array& out)
    {
        if ((depth << 1) > working_processes())
            create_child_task<merge_all_task>(arg_id<0, array>(), arg_id<1, array>());
        else
        {
            size_t half_size = in.size() / 2;
            mes_id<array> in1 = create_message_child<array>(arg_id<0, array>(), new size_t(half_size), new size_t(0));
            mes_id<array> in2 = create_message_child<array>(arg_id<0, array>(), new size_t(in.size() - half_size), new size_t(half_size));

            mes_id<array> out1 = create_message_child<array>(arg_id<1, array>(), new size_t(half_size), new size_t(0));
            mes_id<array> out2 = create_message_child<array>(arg_id<1, array>(), new size_t(out.size() - half_size), new size_t(half_size));

            local_task_id org1 = create_child_task<merge_organizer>(std::make_tuple(out1.as_const(), in1.as_const()), new size_t(depth << 1));
            local_task_id org2 = create_child_task<merge_organizer>(std::make_tuple(out2.as_const(), in2.as_const()), new size_t(depth << 1));
            local_task_id mer = create_child_task<merge_task>(arg_id<0, array>().as_const(), arg_id<1, array>());

            add_dependence(org1, mer);
            add_dependence(org2, mer);
        }
    }
};

class check_task: public task
{
public:
    static bool checking;

    check_task(): task()
    { }

    void operator()(array& a1, array& a2, double t)
    {
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
        std::cout << tm1 - t << std::endl;
    }
};

bool check_task::checking = false;

class init_task: public task
{
public:

    init_task(): task()
    { }

    void operator()(double& t, size_t size)
    {
        std::mt19937 mt(static_cast<int>(time(0)));
        std::uniform_int_distribution<int> uid(0, 10000);

        array& a1 = *new array(size);
        array& a2 = *new array(size);
        array& a3 = *new array(size);

        for (size_t i = 0; i < a1.size(); ++i)
            a1[i] = a2[i] = a3[i] = uid(mt);
        t = MPI_Wtime();

        mes_id<array> m_id1 = add_message(&a1, new size_t(size));
        mes_id<array> m_id2 = add_message(&a2, new size_t(size));
        mes_id<array> m_id3 = add_message(&a3, new size_t(size));

        local_task_id merge = create_task<merge_organizer>(std::make_tuple(m_id1.as_const(), m_id3.as_const()), new size_t(1));
        local_task_id check = create_task<check_task>(m_id3, m_id2, arg_id<0, double>().as_const());

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
        else if (strcmp(argv[i], "-check") == 0)
        {
            check_task::checking = true;
        }
    }

    parallelizer pz;

    pz.execution(new init_task(), std::make_tuple(), new double(0), const_cast<const size_t*>(new size_t(size)));
}
