#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

template<typename Type>
class matrix: public message
{
private:
    Type* arr;
    size_t size_, length_;
    bool created;

public:
    matrix(const size_t& height, const size_t& width): size_(height), length_(width), created(true)
    { arr = new int[size_ * length_]; }

    matrix(const matrix<Type>& m, const size_t& height, const size_t& width, const size_t& offset_h, const size_t& offset_w): size_(height), length_(width), created(false)
    { arr = m.arr + offset_h * length_ + offset_w; }
    
    matrix(const size_t& height, const size_t& width, const size_t& offset_h, const size_t& offset_w): size_(height), length_(width), created(true)
    { arr = new Type[size_ * length_]; }

    void include(const matrix<Type>& m, const size_t& height, const size_t& width, const size_t& offset_h, const size_t& offset_w)
    {
        if (m.created)
            for (size_t i = 0; i < m.size(); ++i)
                for (size_t j = 0; j < m.length(); ++j)
                    arr[(offset_h + i) * length_ + offset_w + j] = m[i][j];
    }

    ~matrix()
    {
        if (created)
            delete[] arr;
    }

    void send(const sender& se) const
    { se.isend(arr, size_ * length_); }

    void recv(const receiver& re)
    { re.irecv(arr, size_ * length_); }

    Type* operator[](size_t n)
    { return arr + length_ * n; }

    const Type* operator[](size_t n) const
    { return arr + length_ * n; }

    size_t size() const
    { return size_; }

    size_t length() const
    { return length_; }
};

template<typename Type>
class multiply_task: public task
{
public:
    multiply_task(): task()
    { }
    void operator()(const matrix<Type>& m, const matrix<Type>& b, matrix<Type>& c)
    {
        for (size_t i = 0; i < c.size(); i++)
            for (size_t j = 0; j < c.length(); j++)
            {
                c[i][j] = 0;
                for (size_t k = 0; k < m.length(); ++k)
                    c[i][j] += m[i][k] * b[k][j];
            }
    }
};

class out_task: public task
{
public:
    static bool checking;

    out_task(): task()
    { }
    void operator()(const matrix<int>& a, const matrix<int>& b, const matrix<int>& c, double start)
    {
        matrix<int> d(c.size(), c.length());

        double t = MPI_Wtime();
        if (checking)
        {
            for (size_t i = 0; i < d.size(); i++)
                for (size_t j = 0; j < d.length(); j++)
                {
                    d[i][j] = 0;
                    for (size_t k = 0; k < a.length(); ++k)
                        d[i][j] += a[i][k] * b[k][j];
                }
            for (size_t i = 0; i < d.size(); i++)
                for (size_t j = 0; j < d.length(); j++)
                    if (c[i][j] != d[i][j])
                    {
                        std::cout << "wrong" << std::endl;
                        goto gh;
                    }
            std::cout << "correct" << std::endl;
        }
        gh:
        std::cout << t - start << std::endl;
    }
};

bool out_task::checking = false;

class init_task: public task
{
    public:
    init_task(): task()
    { }
    void operator()(size_t n, size_t m, size_t l, double& t)
    {
        matrix<int>& a = *new matrix<int>(n, m);
        mes_id<matrix<int>> a_id = add_message(&a, new size_t(n), new size_t(m));

        matrix<int>& b = *new matrix<int>(m, l);
        mes_id<matrix<int>> b_id = add_message(&b, new size_t(m), new size_t(l));

        mes_id<matrix<int>> c_id = create_message<matrix<int>>(new size_t(n), new size_t(l));

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-500, 500);
        for (size_t i = 0; i < a.size(); i++)
            for (size_t j = 0; j < a.length(); ++j)
                a[i][j] = uid(mt);

        for (size_t i = 0; i < b.size(); i++)
            for (size_t j = 0; j < b.length(); ++j)
                b[i][j] = uid(mt);

        t = MPI_Wtime();

        size_t offset = 0;
        for (size_t i = 0; i < working_processes(); i++)
        {
            size_t h = n / working_processes() + ((i < n % working_processes()) ? 1: 0);
            mes_id<matrix<int>> a_child = create_message_child<matrix<int>>(a_id, new size_t(h), new size_t(m), new size_t(offset), new size_t(0));
            mes_id<matrix<int>> c_child = create_message_child<matrix<int>>(c_id, new size_t(h), new size_t(l), new size_t(offset), new size_t(0));
            create_child_task<multiply_task<int>>(a_child.as_const(), b_id.as_const(), c_child);
            offset += h;
        }
        add_dependence(this_task_id<init_task>(), create_task<out_task>(a_id.as_const(), b_id.as_const(), c_id.as_const(), arg_id<3, double>().as_const()));
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    size_t n(100), m(50), l(75);
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            n = atoll(argv[++i]);
            m = atoll(argv[++i]);
            l = atoll(argv[++i]);
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            out_task::checking = true;
        }
    }
    parallelizer pz;

    init_task ti;

    pz.execution(&ti, std::make_tuple(), const_cast<const size_t*>(new size_t(n)), const_cast<const size_t*>(new size_t(m)), const_cast<const size_t*>(new size_t(l)), new double);
}
