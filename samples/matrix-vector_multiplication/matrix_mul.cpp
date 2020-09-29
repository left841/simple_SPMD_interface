#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

template<typename Type>
class vector: public message
{
private:
    size_t size_;
    Type* arr;
    bool created;
public:
    vector(size_t _size): message(), size_(_size), created(true)
    { arr = new Type[size_]; }

    vector(const vector<Type>& parent, const size_t& size, const size_t& offset): size_(size), created(false)
    { arr = parent.arr + offset; }

    vector(const size_t& size, const size_t& offset): size_(size), created(true)
    { arr = new Type[size_]; }

    ~vector()
    {
        if (created)
            delete[] arr;
    }

    void include(const vector<Type>& child, const size_t& size, const size_t& offset)
    {
        if (child.created)
        {
            for (size_t i = 0; i < child.size(); ++i)
                arr[i + offset] = child[i];
        }
    }

    void send(const sender& se) const
    { se.isend(arr, size_); }

    void recv(const receiver& re)
    { re.irecv(arr, size_); }

    Type& operator[](size_t n)
    { return arr[n]; }

    const Type& operator[](size_t n) const
    { return arr[n]; }

    size_t size() const
    { return size_; }
};

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
    void operator()(const matrix<Type>& m, const vector<Type>& b, vector<Type>& c)
    {
        for (size_t i = 0; i < m.size(); ++i)
        {
            c[i] = 0;
            for (size_t j = 0; j < m.length(); ++j)
                c[i] += m[i][j] * b[j];
        }
    }
};

class out_task: public task
{
public:
    static bool checking;

    out_task(): task()
    { }
    void operator()(const matrix<int>& a, const vector<int>& b, const vector<int>& c, double start)
    {
        vector<int> d(c.size());

        double t = MPI_Wtime();
        if (checking)
        {
            for (size_t i = 0; i < a.size(); i++)
            {
                d[i] = 0;
                for (size_t j = 0; j < a.length(); ++j)
                    d[i] += a[i][j] * b[j];
            }
            for (size_t i = 0; i < d.size(); i++)
                if (c[i] != d[i])
                {
                    std::cout << "wrong" << std::endl;
                    goto gh;
                }
            std::cout << "correct" << std::endl;
        }
        gh:
        std::cout <<  t - start << std::endl;
    }
};

bool out_task::checking = false;

class init_task: public task
{
    public:
    init_task(): task()
    { }
    void operator()(size_t n, size_t m, double& t)
    {
        matrix<int>& a = *new matrix<int>(n, m);
        mes_id<matrix<int>> a_id = add_message(&a, new size_t(n), new size_t(m));

        vector<int>& b = *new vector<int>(m);
        mes_id<vector<int>> b_id = add_message(&b, new size_t(m));

        mes_id<vector<int>> c_id = create_message<vector<int>>(new size_t(n));

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-500, 500);
        for (size_t i = 0; i < a.size(); i++)
            for (size_t j = 0; j < a.length(); ++j)
                a[i][j] = uid(mt);

        for (size_t i = 0; i < b.size(); i++)
            b[i] = uid(mt);

        t = MPI_Wtime();

        size_t offset = 0;
        for (size_t i = 0; i < working_processes(); i++)
        {
            size_t h = n / working_processes() + ((i < n % working_processes()) ? 1: 0);
            mes_id<matrix<int>> a_child = create_message_child<matrix<int>>(a_id, new size_t(h), new size_t(m), new size_t(offset), new size_t(0));
            mes_id<vector<int>> c_child = create_message_child<vector<int>>(c_id, new size_t(h), new size_t(offset));
            create_child_task<multiply_task<int>>(std::make_tuple(a_child.as_const(), b_id.as_const(), c_child));
            offset += h;
        }
        add_dependence(this_task_id<init_task>(), create_task<out_task>(std::make_tuple(a_id.as_const(), b_id.as_const(), c_id.as_const(), arg_id<2, double>().as_const())));
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    size_t n(100), m(50);
    for (int i = 1; i < argc; ++i)
    {
        if ((strcmp(argv[i], "-s") == 0) || (strcmp(argv[i], "-size") == 0))
        {
            n = atoll(argv[++i]);
            m = atoll(argv[++i]);
        }
        else if (strcmp(argv[i], "-check") == 0)
        {
            out_task::checking = true;
        }
    }
    parallelizer pz;

    init_task ti;

    pz.execution(&ti, std::make_tuple(), const_cast<const size_t*>(new size_t(n)), const_cast<const size_t*>(new size_t(m)), new double);
}
