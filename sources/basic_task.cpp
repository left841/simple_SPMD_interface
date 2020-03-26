#include "basic_task.h"

namespace auto_parallel
{

    task_creator_base::task_creator_base()
    { }

    task_creator_base::~task_creator_base()
    { }

    task_result::task_result()
    { }

    std::vector<task_data>& task_result::created_tasks()
    { return created_tasks_v; }

    std::vector<message_data>& task_result::created_messages()
    { return created_messages_v; }

    std::vector<message_part_data>& task_result::created_parts()
    { return created_parts_v; }

    task_environment::task_environment(task_data& td)
    { this_task = td; }

    task_environment::task_environment(task_data&& td): this_task(std::move(td))
    { }

    std::vector<task_data>& task_environment::created_tasks()
    { return res.created_tasks(); }

    std::vector<message_data>& task_environment::created_messages()
    { return res.created_messages(); }

    std::vector<message_part_data>& task_environment::created_parts()
    { return res.created_parts(); }

    local_message_id task_environment::get_arg_id(size_t n)
    { return this_task.data[n]; }

    local_message_id task_environment::get_c_arg_id(size_t n)
    { return this_task.c_data[n]; }

    task_data task_environment::get_this_task_data()
    { return this_task; }

    task_result& task_environment::get_result()
    { return res; }

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

    task* task_factory::get(task_type id, std::vector<message*>& data, std::vector<const message*>& c_data)
    { return v.at(id)->get_task(data, c_data); }

}
