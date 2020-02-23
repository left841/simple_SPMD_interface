#include "basic_task.h"

namespace auto_parallel
{

    task_creator_base::task_creator_base()
    { }

    task_creator_base::~task_creator_base()
    { }

    task_environment::task_environment(task_environment::task_data& td)
    { this_task = td; }

    task_environment::task_environment(task_data&& td): this_task(std::move(td))
    { }

    std::vector<task_environment::task_data>& task_environment::get_c_tasks()
    { return created_tasks; }

    std::vector<task_environment::message_data>& task_environment::get_c_messages()
    { return created_messages; }

    std::vector<task_environment::message_part_data>& task_environment::get_c_parts()
    { return created_parts; }

    task_environment::mes_id task_environment::get_arg_id(int n)
    { return this_task.ti->data[n]; }

    task_environment::mes_id task_environment::get_c_arg_id(int n)
    { return this_task.ti->c_data[n]; }

    task_environment::task_data task_environment::get_this_task_data()
    { return this_task; }

    task::task()
    { }

    task::task(std::vector<message*>& mes_v): data(mes_v)
    { }

    task::task(std::vector<message*>& mes_v, std::vector<const message*>& c_mes_v): data(mes_v), c_data(c_mes_v)
    { }

    task::~task()
    { }

    void task::put_a(message* mes)
    { data.push_back(mes); }

    void task::put_c(const message* mes)
    { c_data.push_back(mes); }

    message& task::get_a(size_t id)
    { return *data[id]; }

    const message& task::get_c(size_t id)
    { return *c_data[id]; }

    std::vector<task_creator_base*> task_factory::v;

    task* task_factory::get(size_t id, std::vector<message*>& data, std::vector<const message*>& c_data)
    { return v.at(id)->get_task(data, c_data); }

}
