#include "basic_task.h"

namespace apl
{

    task_environment::task_environment(task_data& td, task_id id): this_task_id({id, TASK_SOURCE::GLOBAL})
    { this_task = td; }

    task_environment::task_environment(task_data&& td, task_id id): this_task(std::move(td)), this_task_id({id, TASK_SOURCE::GLOBAL})
    { }

    std::vector<local_task_id>& task_environment::result_task_ids()
    { return created_tasks_v; }

    std::vector<task_data>& task_environment::created_tasks_simple()
    { return tasks_v; }

    std::vector<task_data>& task_environment::created_child_tasks()
    { return tasks_child_v; }

    std::vector<task_add_data>& task_environment::added_tasks()
    { return tasks_add_v; }

    std::vector<task_add_data>& task_environment::added_child_tasks()
    { return tasks_child_add_v; }

    std::vector<local_message_id>& task_environment::result_message_ids()
    { return created_messages_v; }

    std::vector<message_init_data>& task_environment::created_messages_init()
    { return messages_init_v; }

    std::vector<message_child_data>& task_environment::created_messages_child()
    { return messages_childs_v; }

    std::vector<message_init_add_data>& task_environment::added_messages_init()
    { return messages_init_add_v; }

    std::vector<message_child_add_data>& task_environment::added_messages_child()
    { return messages_childs_add_v; }

    std::vector<task_dependence>& task_environment::created_dependences()
    { return dependence_v; }

    local_message_id task_environment::get_arg_id(size_t n)
    { return this_task.data[n]; }

    local_message_id task_environment::get_c_arg_id(size_t n)
    { return this_task.c_data[n]; }

    task_data task_environment::get_this_task_data()
    { return this_task; }

    void task_environment::add_dependence(local_task_id parent, local_task_id child)
    { dependence_v.push_back({parent, child}); }

    local_task_id task_environment::get_this_task_id()
    { return this_task_id; }

    void task_environment::send(const sender& se)
    {
        size_t sz = created_messages_v.size();
        se.send<size_t>(&sz);
        se.send<size_t>(reinterpret_cast<const size_t*>(created_messages_v.data()), static_cast<int>(sz * 2));
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    se.send(&messages_init_v[i.id].type);
                    messages_init_v[i.id].ii->send(se);
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    se.send(&messages_init_add_v[i.id].type);
                    messages_init_add_v[i.id].ii->send(se);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    se.send(&messages_childs_v[i.id].type);
                    se.send(reinterpret_cast<const size_t*>(&messages_childs_v[i.id].sourse), 2);
                    messages_childs_v[i.id].pi->send(se);
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    se.send(&messages_childs_add_v[i.id].type);
                    se.send(reinterpret_cast<const size_t*>(&messages_childs_add_v[i.id].sourse), 2);
                    messages_childs_add_v[i.id].pi->send(se);
                    break;
                }
            }
        }
        sz = created_tasks_v.size();
        se.send<size_t>(&sz);
        se.send<size_t>(reinterpret_cast<const size_t*>(created_tasks_v.data()), static_cast<int>(sz * 2));
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    se.send(&tasks_v[i.id].type);
                    sz = tasks_v[i.id].data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_v[i.id].data.data()), static_cast<int>(sz * 2));
                    sz = tasks_v[i.id].c_data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_v[i.id].c_data.data()), static_cast<int>(sz * 2));
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    se.send(&tasks_child_v[i.id].type);
                    sz = tasks_child_v[i.id].data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_child_v[i.id].data.data()), static_cast<int>(sz * 2));
                    sz = tasks_child_v[i.id].c_data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_child_v[i.id].c_data.data()), static_cast<int>(sz * 2));
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    se.send(&tasks_add_v[i.id].type);
                    sz = tasks_add_v[i.id].data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_add_v[i.id].data.data()), static_cast<int>(sz * 2));
                    sz = tasks_add_v[i.id].c_data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_add_v[i.id].c_data.data()), static_cast<int>(sz * 2));
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    se.send(&tasks_child_add_v[i.id].type);
                    sz = tasks_child_add_v[i.id].data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_child_add_v[i.id].data.data()), static_cast<int>(sz * 2));
                    sz = tasks_child_add_v[i.id].c_data.size();
                    se.send(&sz);
                    se.isend(reinterpret_cast<const size_t*>(tasks_child_add_v[i.id].c_data.data()), static_cast<int>(sz * 2));
                    break;
                }
            }
        }
        sz = dependence_v.size();
        se.send(&sz);
        se.isend(reinterpret_cast<const size_t*>(dependence_v.data()), static_cast<int>(sz * 4));
    }

    void task_environment::recv(const receiver& re)
    {
        size_t sz;
        re.recv(&sz);
        created_messages_v.resize(sz);
        re.recv(reinterpret_cast<size_t*>(created_messages_v.data()), static_cast<int>(sz * 2));
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data d;
                    re.recv(&d.type);
                    d.ii = message_init_factory::get_info(d.type);
                    d.ii->recv(re);
                    messages_init_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data d;
                    re.recv(&d.type);
                    d.ii = message_init_factory::get_info(d.type);
                    d.ii->recv(re);
                    d.mes = nullptr;
                    messages_init_add_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    message_child_data d;
                    re.recv(&d.type);
                    re.recv(reinterpret_cast<size_t*>(&d.sourse), 2);
                    d.pi = message_child_factory::get_info(d.type);
                    d.pi->recv(re);
                    messages_childs_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    message_child_add_data d;
                    re.recv(&d.type);
                    re.recv(reinterpret_cast<size_t*>(&d.sourse), 2);
                    d.pi = message_child_factory::get_info(d.type);
                    d.pi->recv(re);
                    d.mes = nullptr;
                    messages_childs_add_v.push_back(d);
                    break;
                }
            }
        }
        re.recv(&sz);
        created_tasks_v.resize(sz);
        re.recv<size_t>(reinterpret_cast<size_t*>(created_tasks_v.data()), static_cast<int>(sz * 2));
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    task_data d;
                    re.recv(&d.type);
                    re.recv(&sz);
                    d.data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.data.data()), static_cast<int>(sz * 2));
                    re.recv(&sz);
                    d.c_data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.c_data.data()), static_cast<int>(sz * 2));
                    tasks_v.push_back(d);
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    task_data d;
                    re.recv(&d.type);
                    re.recv(&sz);
                    d.data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.data.data()), static_cast<int>(sz * 2));
                    re.recv(&sz);
                    d.c_data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.c_data.data()), static_cast<int>(sz * 2));
                    tasks_child_v.push_back(d);
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data d;
                    re.recv(&d.type);
                    re.recv(&sz);
                    d.data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.data.data()), static_cast<int>(sz * 2));
                    re.recv(&sz);
                    d.c_data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.c_data.data()), static_cast<int>(sz * 2));
                    d.t = nullptr;
                    tasks_add_v.push_back(d);
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data d;
                    re.recv(&d.type);
                    re.recv(&sz);
                    d.data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.data.data()), static_cast<int>(sz * 2));
                    re.recv(&sz);
                    d.c_data.resize(sz);
                    re.recv(reinterpret_cast<size_t*>(d.c_data.data()), static_cast<int>(sz * 2));
                    d.t = nullptr;
                    tasks_child_add_v.push_back(d);
                    break;
                }
            }
        }
        re.recv(&sz);
        dependence_v.resize(sz);
        re.irecv(reinterpret_cast<size_t*>(dependence_v.data()), static_cast<int>(sz * 4));
    }

    task::task()
    { }

    task::task(const std::vector<message*>& mes_v): data(mes_v)
    { }

    task::task(const std::vector<message*>& mes_v, const std::vector<const message*>& c_mes_v): data(mes_v), c_data(c_mes_v)
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

    task_factory::creator_base::creator_base()
    { }

    task_factory::creator_base::~creator_base()
    { }

    std::vector<task_factory::creator_base*>& task_factory::task_vec()
    {
        static std::vector<creator_base*> v;
        return v;
    }

    task* task_factory::get(task_type id, std::vector<message*>& data, std::vector<const message*>& c_data)
    { return task_vec().at(id)->get_task(data, c_data); }

}
