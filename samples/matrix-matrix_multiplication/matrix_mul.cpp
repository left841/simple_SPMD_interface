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

struct size_type: public message
{
    size_t size;

    size_type(size_t s = 0): size(s)
    { }

    operator size_t()
    { return size; }

    operator const size_t() const
    { return size; }

    void send(const sender& se)
    { se.send(&size); }

    void recv(const receiver& re)
    { re.recv(&size); }
};

struct matrix_size: public message
{
    size_t sizes[2];

    matrix_size(): sizes()
    { }

    matrix_size(size_t sizes_[2])
    {
        sizes[0] = sizes_[0];
        sizes[1] = sizes_[1];
    }

    matrix_size(size_t s, size_t l)
    {
        sizes[0] = s;
        sizes[1] = l;
    }

    void send(const sender& se)
    { se.send(sizes, 2); }

    void recv(const receiver& re)
    { re.recv(sizes, 2); }
};

struct matrix_part: public message
{
    size_t info[4];

    matrix_part(): info()
    { }

    matrix_part(size_t sizes[2], size_t offsets[2])
    {
        info[0] = sizes[0];
        info[1] = sizes[1];
        info[2] = offsets[0];
        info[3] = offsets[1];
    }

    void send(const sender& se)
    { se.send(info, 4); }

    void recv(const receiver& re)
    { re.recv(info, 4); }
};

template<typename Type>
class matrix: public message
{
private:
    Type* arr;
    size_t size_, length_;
    bool created;

public:
    matrix(size_t _s, size_t _l): message(), length_(_l), size_(_s), created(true)
    { arr = new int[size_ * length_]; }

    matrix(const matrix_size& info): size_(info.sizes[0]), length_(info.sizes[1]), created(true)
    { arr = new int[size_ * length_]; }

    matrix(const matrix<Type>& m, const matrix_part& p): size_(p.info[0]), length_(p.info[1]), created(false)
    { arr = m.arr + p.info[2] * length_ + p.info[3]; }
    
    matrix(const matrix_part& p): size_(p.info[0]), length_(p.info[1]), created(true)
    { arr = new Type[size_ * length_]; }

    void include(const matrix<Type>& m, const matrix_part& p)
    {
        if (m.created)
            for (size_t i = 0; i < m.size(); ++i)
                for (size_t j = 0; j < m.length(); ++j)
                    arr[(p.info[2] + i) * length_ + p.info[3] + j] = m[i][j];
    }

    ~matrix()
    {
        if (created)
            delete[] arr;
    }

    void send(const sender& se)
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
    multiply_task(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        const matrix<Type>& m = dynamic_cast<const matrix<Type>&>(const_arg(0));
        const matrix<Type>& b = dynamic_cast<const matrix<Type>&>(const_arg(1));
        matrix<Type>& c = dynamic_cast<matrix<Type>&>(arg(0));

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

    out_task(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        const matrix<int>& a = dynamic_cast<const matrix<int>&>(const_arg(0));
        const matrix<int>& b = dynamic_cast<const matrix<int>&>(const_arg(1));
        const matrix<int>& c = dynamic_cast<const matrix<int>&>(const_arg(2));
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
        std::cout << t - dynamic_cast<const time_cl&>(const_arg(3)).time << std::endl;
    }
};

bool out_task::checking = false;

class init_task: public task
{
    public:
    init_task(const std::vector<message*>& mes_v, const std::vector<const message*>& cmes_v): task(mes_v, cmes_v)
    { }
    void perform()
    {
        size_t n = dynamic_cast<const size_type&>(const_arg(0));
        size_t m = dynamic_cast<const size_type&>(const_arg(1));
        size_t l = dynamic_cast<const size_type&>(const_arg(2));
        size_t sizes[2] = {n, m};
        matrix<int>& a = *new matrix<int>(n, m);
        local_message_id a_id = add_message_init(&a, new matrix_size(sizes));

        matrix<int>& b = *new matrix<int>(m, l);
        local_message_id b_id = add_message_init(&b, new matrix_size(m, l));

        local_message_id c_id = create_message_init<matrix<int>>(new matrix_size(n, l));

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-500, 500);
        for (size_t i = 0; i < a.size(); i++)
            for (size_t j = 0; j < a.length(); ++j)
                a[i][j] = uid(mt);

        for (size_t i = 0; i < b.size(); i++)
            for (size_t j = 0; j < b.length(); ++j)
                b[i][j] = uid(mt);

        time_cl& t = dynamic_cast<time_cl&>(arg(0));
        t.time = MPI_Wtime();

        size_t offsets[2] = {0, 0};
        for (size_t i = 0; i < working_processes(); i++)
        {
            sizes[0] = n / working_processes() + ((i < n % working_processes()) ? 1: 0);
            sizes[1] = m;
            local_message_id a_child = create_message_child<matrix<int>>(a_id, new matrix_part(sizes, offsets));
            sizes[1] = l;
            local_message_id c_child = create_message_child<matrix<int>>(c_id, new matrix_part(sizes, offsets));
            create_child_task<multiply_task<int>>({c_child}, {a_child, b_id});
            offsets[0] += sizes[0];
        }
        add_dependence(this_task_id(), create_task<out_task>({}, {a_id, b_id, c_id, arg_id(0)}));
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    size_type n(100), m(50), l(75);
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

    time_cl t;
    init_task ti({&t}, {&n, &m, &l});

    pz.execution(&ti);
}
