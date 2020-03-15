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

class my_vector: public message
{
private:
    int* ptr;
    int count;
    bool is_created;
public:
    class init_info: public init_info_base
    {
    public:
        int new_size;

        void send(const sender& se)
        {
            se.send(&new_size, 1, MPI_INT);
        }
        void recv(const receiver& re)
        {
            re.recv(&new_size, 1, MPI_INT);
        }
    };
    class part_info: public part_info_base
    {
    public:
        int new_size;
        int offset;

        void send(const sender& se)
        {
            se.send(&new_size, 1, MPI_INT);
            se.send(&offset, 1, MPI_INT);
        }
        void recv(const receiver& re)
        {
            re.recv(&new_size, 1, MPI_INT);
            re.recv(&offset, 1, MPI_INT);
        }
    };

    my_vector(int new_size = 0): ptr(nullptr), count(new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }
    my_vector(init_info* ii): ptr(nullptr), count(ii->new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }
    my_vector(message* src, part_info* pi) : ptr(nullptr), count(pi->new_size), is_created(false)
    {
        ptr = dynamic_cast<my_vector*>(src)->ptr + pi->offset;
    }

    ~my_vector()
    {
        if (is_created)
            delete[] ptr;
    }

    int& operator[](size_t pos)
    {
        return ptr[pos];
    }

    const int& operator[](size_t pos) const
    {
        return ptr[pos];
    }

    int size() const
    {
        return count;
    }
    void send(const sender& se)
    {
        se.send(ptr, count, MPI_INT);
    }
    void recv(const receiver& re)
    {
        re.recv(ptr, count, MPI_INT);
    }
};


class vector_sum: public task
{
public:
    vector_sum(std::vector<message*> mes_v, std::vector<const message*> const_mes_v): task(mes_v, const_mes_v)
    { }

    void perform(task_environment& env)
    {
        const my_vector& first = *dynamic_cast<const my_vector*>(c_data[0]);
        const my_vector& second = *dynamic_cast<const my_vector*>(c_data[1]);
        my_vector& output = *dynamic_cast<my_vector*>(data[0]);

        for (int i = 0; i < output.size(); ++i)
            output[i] = first[i] + second[i];

    }
};


class vector_sum_init: public task
{
private:
    int part_count;
public:
    vector_sum_init(std::vector<message*> mes_v, std::vector<const message*> const_mes_v) : task(mes_v, const_mes_v)
    {
        part_count = 4;
    }

    void perform(task_environment& env)
    {
        my_vector& first = *dynamic_cast<my_vector*>(data[0]);
        my_vector& second = *dynamic_cast<my_vector*>(data[1]);
        const my_vector& output = *dynamic_cast<const my_vector*>(c_data[0]);

        std::mt19937 mt(time(0));
        std::uniform_int_distribution<int> uid(-50000, 50000);
        for (int i = 0; i < first.size(); ++i)
            first[i] = uid(mt);
        for (int i = 0; i < second.size(); ++i)
            second[i] = uid(mt);

        int offset = 0;
        for (int i = 0; i < part_count; ++i)
        {
            int new_size = first.size() / part_count + ((first.size() % part_count > i) ? 1 : 0);

            my_vector::init_info* ii_first = new my_vector::init_info;
            ii_first->new_size = new_size;

            my_vector::part_info* pi_first = new my_vector::part_info;
            pi_first->new_size = new_size;
            pi_first->offset = offset;

            my_vector::init_info* ii_second = new my_vector::init_info;
            my_vector::part_info* pi_second = new my_vector::part_info;
            *ii_second = *ii_first;
            *pi_second = *pi_first;

            my_vector::init_info* ii_output = new my_vector::init_info;
            my_vector::part_info* pi_output = new my_vector::part_info;
            *ii_output = *ii_first;
            *pi_output = *pi_first;

            task_environment::mes_id id_first = env.create_message<my_vector>(ii_first, pi_first, env.get_arg_id(0));
            task_environment::mes_id id_second = env.create_message<my_vector>(ii_second, pi_second, env.get_arg_id(1));
            task_environment::mes_id id_output = env.create_message<my_vector>(ii_output, pi_output, env.get_c_arg_id(0));

            task_environment::task_info* ti = new task_environment::task_info;
            ti->data = {id_output};
            ti->c_data = {id_first, id_second};

            env.create_task<vector_sum>(ti);

            offset += new_size;
        }
    }
};

class end_task: public task
{
public:
    end_task(std::vector<message*> mes_v, std::vector<const message*> const_mes_v): task(mes_v, const_mes_v)
    { }

    void perform(task_environment& env)
    {
        const my_vector& first = *dynamic_cast<const my_vector*>(c_data[0]);
        const my_vector& second = *dynamic_cast<const my_vector*>(c_data[1]);
        const my_vector& output = *dynamic_cast<const my_vector*>(c_data[2]);
        
        my_vector check(output.size());

        for (int i = 0; i < check.size(); ++i)
            check[i] = first[i] + second[i];

        for (int i = 0; i < check.size(); ++i)
            if (output[i] != check[i])
            {
                std::cout << "error" << std::endl;
                return;
            }
        std::cout << "ok" << std::endl;
    }
};

int main(int argc, char** argv)
{
    int size = 10000000;

    parallel_engine pe(&argc, &argv);

    message_factory::add<my_vector>();
    message_factory::add_part<my_vector>();

    task_factory::add<vector_sum>();

    parallelizer pz;

    task_graph tg;
    
    my_vector* first = new my_vector(size);
    my_vector* second = new my_vector(size);
    my_vector* output = new my_vector(size);

    vector_sum_init* vsi_task = new vector_sum_init({first, second}, {output});

    end_task* e_task = new end_task({}, {first, second, output});

    tg.add_dependence(vsi_task, e_task);

    pz.init(tg);

    pz.execution();

    delete first;
    delete second;
    delete output;
    delete vsi_task;
    delete e_task;
}
