#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include "parallel.h"
using namespace apl;

struct size_info: public message
{
public:
    size_t new_size;

    size_info(size_t sz = 0): message(), new_size(sz)
    { }

    void send(const sender& se)
    { se.send(&new_size); }

    void recv(const receiver& re)
    { re.recv(&new_size); }
};

struct part_info: public sendable
{
public:
    size_t new_size;
    size_t offset;

    part_info(size_t sz = 0, size_t off = 0): sendable(), new_size(sz), offset(off)
    { }

    void send(const sender& se)
    {
        se.send(&new_size);
        se.send(&offset);
    }
    void recv(const receiver& re)
    {
        re.recv(&new_size);
        re.recv(&offset);
    }
};

class my_vector: public message
{
private:
    int* ptr;
    size_t count;
    bool is_created;
public:

    my_vector(const part_info& pi): message(), ptr(nullptr), count(pi.new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }

    my_vector(const size_info& si): message(), ptr(nullptr), count(si.new_size), is_created(true)
    {
        if (count > 0)
            ptr = new int[count];
    }

    my_vector(const my_vector& src, const part_info& pi): message(), ptr(nullptr), count(pi.new_size), is_created(false)
    { ptr = src.ptr + pi.offset; }

    ~my_vector()
    {
        if (is_created && (count > 0))
            delete[] ptr;
    }

    void include(const my_vector& child, const part_info& pi)
    {
        if (is_created)
            for (size_t i = 0; i < child.size(); ++i)
                ptr[pi.offset + i] = child[i];
    }

    int& operator[](size_t pos)
    { return ptr[pos]; }

    const int& operator[](size_t pos) const
    { return ptr[pos]; }

    size_t size() const
    { return count; }

    void send(const sender& se)
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

        my_vector check(size_info(output.size()));

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
        const size_info& size = dynamic_cast<const size_info&>(const_arg(0));

        my_vector& first = *new my_vector(size);
        my_vector& second = *new my_vector(size);

        std::mt19937 mt(static_cast<unsigned>(time(0)));
        std::uniform_int_distribution<int> uid(-50000, 50000);
        for (size_t i = 0; i < first.size(); ++i)
            first[i] = uid(mt);
        for (size_t i = 0; i < second.size(); ++i)
            second[i] = uid(mt);

        local_message_id first_id = add_message_init(&first, new size_info(size));
        local_message_id second_id = add_message_init(&second, new size_info(size));
        local_message_id output_id = create_message_init<my_vector>(new size_info(size));

        int offset = 0;
        for (int i = 0; i < working_processes(); ++i)
        {
            int new_size = size.new_size / working_processes() + ((size.new_size % working_processes() > i) ? 1 : 0);

            local_message_id first_child = create_message_child<my_vector>(first_id, new part_info(new_size, offset));
            local_message_id second_child = create_message_child<my_vector>(second_id, new part_info(new_size, offset));
            local_message_id output_child = create_message_child<my_vector>(output_id, new part_info(new_size, offset));

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

    size_info size(10000000);
    vector_sum_init vsi_task({}, {&size});

    pz.execution(&vsi_task);
}
