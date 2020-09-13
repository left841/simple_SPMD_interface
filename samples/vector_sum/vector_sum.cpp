#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

class my_vector: public message
{
private:
    int* ptr;
    size_t count;
    bool is_created;
public:

    my_vector(const size_t& new_size, const size_t& offset): message(), ptr(nullptr), count(new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }

    my_vector(const size_t& new_size): message(), ptr(nullptr), count(new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }

    my_vector(const my_vector& src, const size_t& new_size, const size_t& offset): message(), ptr(nullptr), count(new_size), is_created(false)
    { ptr = src.ptr + offset; }

    ~my_vector()
    {
        if (is_created && (count > 0))
            delete[] ptr;
    }

    void include(const my_vector& child, const size_t& new_size, const size_t& offset)
    {
        if (is_created)
            for (size_t i = 0; i < child.size(); ++i)
                ptr[offset + i] = child[i];
    }

    int& operator[](size_t pos)
    { return ptr[pos]; }

    const int& operator[](size_t pos) const
    { return ptr[pos]; }

    size_t size() const
    { return count; }

    void send(const sender& se) const
    { se.send(ptr, count); }

    void recv(const receiver& re)
    { re.recv(ptr, count); }
};


class vector_sum: public task
{
public:
    vector_sum(std::vector<message*> mes_v, std::vector<const message*> const_mes_v): task(mes_v, const_mes_v)
    { }

    void perform()
    {
        const my_vector& first = dynamic_cast<const my_vector&>(const_arg(0));
        const my_vector& second = dynamic_cast<const my_vector&>(const_arg(1));
        my_vector& output = dynamic_cast<my_vector&>(arg(0));

        for (size_t i = 0; i < output.size(); ++i)
            output[i] = first[i] + second[i];
    }
};

class end_task: public task
{
public:
    end_task(std::vector<message*> mes_v, std::vector<const message*> const_mes_v): task(mes_v, const_mes_v)
    { }

    void perform()
    {
        const my_vector& first = dynamic_cast<const my_vector&>(const_arg(0));
        const my_vector& second = dynamic_cast<const my_vector&>(const_arg(1));
        const my_vector& output = dynamic_cast<const my_vector&>(const_arg(2));

        my_vector check(output.size());

        for (size_t i = 0; i < check.size(); ++i)
            check[i] = first[i] + second[i];

        for (size_t i = 0; i < check.size(); ++i)
            if (output[i] != check[i])
            {
                std::cout << "error" << std::endl;
                return;
            }
        std::cout << "ok" << std::endl;
    }
};

class vector_sum_init: public task
{
public:
    vector_sum_init(std::vector<message*> mes_v, std::vector<const message*> const_mes_v): task(mes_v, const_mes_v)
    { }

    void perform()
    {
        size_t size = dynamic_cast<const message_wrapper<size_t>&>(const_arg(0));

        my_vector& first = *new my_vector(size);
        my_vector& second = *new my_vector(size);

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-50000, 50000);
        for (size_t i = 0; i < first.size(); ++i)
            first[i] = uid(mt);
        for (size_t i = 0; i < second.size(); ++i)
            second[i] = uid(mt);

        local_message_id first_id = add_message_init(&first, new size_t(size));
        local_message_id second_id = add_message_init(&second, new size_t(size));
        local_message_id output_id = create_message_init<my_vector>(new size_t(size));

        int offset = 0;
        for (int i = 0; i < working_processes(); ++i)
        {
            int new_size = size / working_processes() + ((size % working_processes() > i) ? 1 : 0);

            local_message_id first_child = create_message_child<my_vector>(first_id, new size_t(new_size), new size_t(offset));
            local_message_id second_child = create_message_child<my_vector>(second_id, new size_t(new_size), new size_t(offset));
            local_message_id output_child = create_message_child<my_vector>(output_id, new size_t(new_size), new size_t(offset));

            create_child_task<vector_sum>({output_child}, {first_child, second_child});

            offset += new_size;
        }

        add_dependence(this_task_id(), create_task<end_task>({}, {first_id, second_id, output_id}));
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    parallelizer pz;

    vector_sum_init vsi_task({}, {new message_wrapper<size_t>(new size_t(10000000))});

    pz.execution(&vsi_task);
}
