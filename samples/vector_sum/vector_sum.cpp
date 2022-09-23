#include <cstring>
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
    void operator()(const my_vector& first, const my_vector& second, my_vector& output)
    {
        for (size_t i = 0; i < output.size(); ++i)
            output[i] = first[i] + second[i];
    }
};

class end_task: public task
{
public:
    void operator()(const my_vector& first, const my_vector& second, const my_vector& output)
    {
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
    void operator()(size_t size)
    {
        my_vector& first = *new my_vector(size);
        my_vector& second = *new my_vector(size);

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-50000, 50000);
        for (size_t i = 0; i < first.size(); ++i)
            first[i] = uid(mt);
        for (size_t i = 0; i < second.size(); ++i)
            second[i] = uid(mt);

        mes_id<my_vector> first_id = add_message(&first, new size_t(size));
        mes_id<my_vector> second_id = add_message(&second, new size_t(size));
        mes_id<my_vector> output_id = create_message<my_vector>(new size_t(size));

        size_t offset = 0;
        for (int i = 0; i < get_workers_count(); ++i)
        {
            size_t new_size = size / get_workers_count() + ((size % get_workers_count() > i) ? 1 : 0);

            mes_id<my_vector> first_child = create_message_child<my_vector>(first_id, new size_t(new_size), new size_t(offset));
            mes_id<my_vector> second_child = create_message_child<my_vector>(second_id, new size_t(new_size), new size_t(offset));
            mes_id<my_vector> output_child = create_message_child<my_vector>(output_id, new size_t(new_size), new size_t(offset));

            create_child_task<vector_sum>(first_child.as_const(), second_child.as_const(), output_child);

            offset += new_size;
        }

        add_dependence(this_task_id<vector_sum_init>(), create_task<end_task>(first_id.as_const(), second_id.as_const(), output_id.as_const()));
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    parallelizer pz;

    size_t size = 10000000;
    vector_sum_init vsi_task;

    pz.execution(&vsi_task, std::make_tuple(), const_cast<const size_t*>(&size));
}
