#include "apl/task.h"

namespace apl
{

    task_environment::task_environment(): this_task_id({{0, MESSAGE_SOURCE::GLOBAL}, 0, TASK_SOURCE::GLOBAL}), proc_count(0)
    { }

    task_environment::task_environment(task_data& td): this_task_id({{0, MESSAGE_SOURCE::GLOBAL}, 0, TASK_SOURCE::GLOBAL}), proc_count(0)
    {
        this_task = td;
        set_all_task_data();
    }

    task_environment::task_environment(task_data&& td): this_task(std::move(td)), this_task_id({{0, MESSAGE_SOURCE::GLOBAL}, 0, TASK_SOURCE::GLOBAL}), proc_count(0)
    { set_all_task_data(); }

    void task_environment::set_all_task_data()
    {
        size_t i = 0, j = 0;
        for (size_t k = 0; k < task_factory::const_map(this_task.type).size(); ++k)
        {
            if (task_factory::const_map(this_task.type)[k])
                all_task_data.push_back(this_task.c_data[i++]);
            else
                all_task_data.push_back(this_task.data[j++]);
        }
    }

    std::vector<local_task_id>& task_environment::result_task_ids()
    { return created_tasks_v; }

    std::vector<task_data>& task_environment::created_tasks_simple()
    { return tasks_v; }

    std::vector<task_data>& task_environment::created_child_tasks()
    { return tasks_child_v; }

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

    local_message_id task_environment::arg_id(size_t n) const
    { return all_task_data[n]; }

    task_data task_environment::get_this_task_data() const
    { return this_task; }

    local_message_id task_environment::create_message_init(message_type type, const std::vector<message*>& info)
    {
        messages_init_v.push_back({type, info});
        created_messages_v.push_back({messages_init_v.size() - 1, MESSAGE_SOURCE::INIT});
        return created_messages_v.back();
    }

    local_message_id task_environment::create_message_child(message_type type, local_message_id source, const std::vector<message*>& info)
    {
        messages_childs_v.push_back({type, source, info});
        created_messages_v.push_back({messages_childs_v.size() - 1, MESSAGE_SOURCE::CHILD});
        return created_messages_v.back();
    }

    local_message_id task_environment::add_message_init(message_type type, message* m, const std::vector<message*>& info)
    {
        messages_init_add_v.push_back({type, info, m});
        created_messages_v.push_back({messages_init_add_v.size() - 1, MESSAGE_SOURCE::INIT_A});
        return created_messages_v.back();
    }

    local_message_id task_environment::add_message_child(message_type type, message* m, local_message_id source, const std::vector<message*>& info)
    {
        messages_childs_add_v.push_back({type, source, info, m});
        created_messages_v.push_back({messages_childs_add_v.size() - 1, MESSAGE_SOURCE::CHILD_A});
        return created_messages_v.back();
    }

    local_task_id task_environment::create_task(task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data, const std::vector<message*>& info)
    {
        tasks_v.push_back({type.pt, data, const_data});
        created_tasks_v.push_back({create_message_init(type.mt, info) ,tasks_v.size() - 1, TASK_SOURCE::INIT});
        return created_tasks_v.back();
    }

    local_task_id task_environment::create_child_task(task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data, const std::vector<message*>& info)
    {
        tasks_child_v.push_back({type.pt, data, const_data});
        created_tasks_v.push_back({create_message_init(type.mt, info), tasks_child_v.size() - 1, TASK_SOURCE::CHILD});
        return created_tasks_v.back();
    }

    local_task_id task_environment::add_task(perform_type type, local_message_id t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_v.push_back({type, data, const_data});
        created_tasks_v.push_back({t, tasks_v.size() - 1, TASK_SOURCE::INIT});
        return created_tasks_v.back();
    }

    local_task_id task_environment::add_child_task(perform_type type, local_message_id t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data)
    {
        tasks_child_v.push_back({type, data, const_data});
        created_tasks_v.push_back({t, tasks_child_v.size() - 1, TASK_SOURCE::CHILD});
        return created_tasks_v.back();
    }

    void task_environment::add_dependence(local_task_id parent, local_task_id child)
    { dependence_v.push_back({parent, child}); }

    local_task_id task_environment::get_this_task_id() const
    { return this_task_id; }

    void task_environment::set_proc_count(size_t sz)
    { proc_count = sz; }

    size_t task_environment::working_processes() const
    { return proc_count; }

    void task_environment::send(const sender& se) const
    {
        se.send<local_message_id>(created_messages_v.data(), created_messages_v.size());
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    se.send(messages_init_v[i.id].type);
                    for (message* p: messages_init_v[i.id].ii)
                        p->send(se);
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    se.send(messages_init_add_v[i.id].type);
                    for (message* p: messages_init_add_v[i.id].ii)
                        p->send(se);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    se.send(messages_childs_v[i.id].type);
                    se.send(messages_childs_v[i.id].sourse);
                    for (message* p: messages_childs_v[i.id].pi)
                        p->send(se);
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    se.send(messages_childs_add_v[i.id].type);
                    se.send(messages_childs_add_v[i.id].sourse);
                    for (message* p: messages_childs_add_v[i.id].pi)
                        p->send(se);
                    break;
                }
                default:
                    abort();
            }
        }
        se.send<local_task_id>(created_tasks_v.data(), created_tasks_v.size());
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    se.send(tasks_v[i.id].type);
                    se.send(tasks_v[i.id].data.data(), tasks_v[i.id].data.size());
                    se.send(tasks_v[i.id].c_data.data(), tasks_v[i.id].c_data.size());
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    se.send(tasks_child_v[i.id].type);
                    se.send(tasks_child_v[i.id].data.data(), tasks_child_v[i.id].data.size());
                    se.send(tasks_child_v[i.id].c_data.data(), tasks_child_v[i.id].c_data.size());
                    break;
                }
                default:
                    abort();
            }
        }
        se.send(dependence_v.data(), dependence_v.size());
    }

    void task_environment::recv(const receiver& re)
    {
        created_messages_v.resize(re.probe<local_message_id>());
        re.recv(created_messages_v.data(), created_messages_v.size());
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data d;
                    re.recv(d.type);
                    d.ii = message_init_factory::get_info(d.type);
                    for (message* p: d.ii)
                        p->recv(re);
                    messages_init_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data d;
                    re.recv(d.type);
                    d.ii = message_init_factory::get_info(d.type);
                    for (message* p: d.ii)
                        p->recv(re);
                    d.mes = nullptr;
                    messages_init_add_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    message_child_data d;
                    re.recv(d.type);
                    re.recv(d.sourse);
                    d.pi = message_child_factory::get_info(d.type);
                    for (message* p: d.pi)
                        p->recv(re);
                    messages_childs_v.push_back(d);
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    message_child_add_data d;
                    re.recv(d.type);
                    re.recv(d.sourse);
                    d.pi = message_child_factory::get_info(d.type);
                    for (message* p: d.pi)
                        p->recv(re);
                    d.mes = nullptr;
                    messages_childs_add_v.push_back(d);
                    break;
                }
                default:
                    abort();
            }
        }
        created_tasks_v.resize(re.probe<local_task_id>());
        re.recv<local_task_id>(created_tasks_v.data(), created_tasks_v.size());
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data d;
                    re.recv(d.type);
                    d.data.resize(re.probe<local_message_id>());
                    re.recv(d.data.data(), d.data.size());
                    d.c_data.resize(re.probe<local_message_id>());
                    re.recv(d.c_data.data(), d.c_data.size());
                    tasks_v.push_back(d);
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data d;
                    re.recv(d.type);
                    d.data.resize(re.probe<local_message_id>());
                    re.recv(d.data.data(), d.data.size());
                    d.c_data.resize(re.probe<local_message_id>());
                    re.recv(d.c_data.data(), d.c_data.size());
                    tasks_child_v.push_back(d);
                    break;
                }
                default:
                    abort();
            }
        }
        dependence_v.resize(re.probe<task_dependence>());
        re.recv(dependence_v.data(), dependence_v.size());
    }

    void task_environment::isend(const sender& se, request_block& req) const
    {
        se.send<local_message_id>(created_messages_v.data(), created_messages_v.size());
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
            case MESSAGE_SOURCE::INIT:
            {
                se.send(messages_init_v[i.id].type);
                for (message* p: messages_init_v[i.id].ii)
                    p->send(se);
                break;
            }
            case MESSAGE_SOURCE::INIT_A:
            {
                se.send(messages_init_add_v[i.id].type);
                for (message* p: messages_init_add_v[i.id].ii)
                    p->send(se);
                break;
            }
            case MESSAGE_SOURCE::CHILD:
            {
                se.send(messages_childs_v[i.id].type);
                se.send(messages_childs_v[i.id].sourse);
                for (message* p: messages_childs_v[i.id].pi)
                    p->send(se);
                break;
            }
            case MESSAGE_SOURCE::CHILD_A:
            {
                se.send(messages_childs_add_v[i.id].type);
                se.send(messages_childs_add_v[i.id].sourse);
                for (message* p: messages_childs_add_v[i.id].pi)
                    p->send(se);
                break;
            }
            default:
                abort();
            }
        }
        se.send<local_task_id>(created_tasks_v.data(), created_tasks_v.size());
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
            case TASK_SOURCE::INIT:
            {
                se.send(tasks_v[i.id].type);
                se.isend(tasks_v[i.id].data.data(), tasks_v[i.id].data.size(), req);
                se.isend(tasks_v[i.id].c_data.data(), tasks_v[i.id].c_data.size(), req);
                break;
            }
            case TASK_SOURCE::CHILD:
            {
                se.send(tasks_child_v[i.id].type);
                se.isend(tasks_child_v[i.id].data.data(), tasks_child_v[i.id].data.size(), req);
                se.isend(tasks_child_v[i.id].c_data.data(), tasks_child_v[i.id].c_data.size(), req);
                break;
            }
            default:
                abort();
            }
        }
        se.isend(dependence_v.data(), dependence_v.size(), req);
    }

    void task_environment::irecv(const receiver& re, request_block& req)
    {
        created_messages_v.resize(re.probe<local_message_id>());
        re.recv(created_messages_v.data(), created_messages_v.size());
        for (const local_message_id& i: created_messages_v)
        {
            switch (i.src)
            {
            case MESSAGE_SOURCE::INIT:
            {
                message_init_data d;
                re.recv(d.type);
                d.ii = message_init_factory::get_info(d.type);
                for (message* p: d.ii)
                    p->recv(re);
                messages_init_v.push_back(d);
                break;
            }
            case MESSAGE_SOURCE::INIT_A:
            {
                message_init_add_data d;
                re.recv(d.type);
                d.ii = message_init_factory::get_info(d.type);
                for (message* p: d.ii)
                    p->recv(re);
                d.mes = nullptr;
                messages_init_add_v.push_back(d);
                break;
            }
            case MESSAGE_SOURCE::CHILD:
            {
                message_child_data d;
                re.recv(d.type);
                re.recv(d.sourse);
                d.pi = message_child_factory::get_info(d.type);
                for (message* p: d.pi)
                    p->recv(re);
                messages_childs_v.push_back(d);
                break;
            }
            case MESSAGE_SOURCE::CHILD_A:
            {
                message_child_add_data d;
                re.recv(d.type);
                re.recv(d.sourse);
                d.pi = message_child_factory::get_info(d.type);
                for (message* p: d.pi)
                    p->recv(re);
                d.mes = nullptr;
                messages_childs_add_v.push_back(d);
                break;
            }
            default:
                abort();
            }
        }
        created_tasks_v.resize(re.probe<local_task_id>());
        re.recv<local_task_id>(created_tasks_v.data(), created_tasks_v.size());
        for (const local_task_id& i: created_tasks_v)
        {
            switch (i.src)
            {
            case TASK_SOURCE::INIT:
            {
                task_data d;
                re.recv(d.type);
                d.data.resize(re.probe<local_message_id>());
                re.recv(d.data.data(), d.data.size());
                d.c_data.resize(re.probe<local_message_id>());
                re.recv(d.c_data.data(), d.c_data.size());
                tasks_v.push_back(d);
                break;
            }
            case TASK_SOURCE::CHILD:
            {
                task_data d;
                re.recv(d.type);
                d.data.resize(re.probe<local_message_id>());
                re.recv(d.data.data(), d.data.size());
                d.c_data.resize(re.probe<local_message_id>());
                re.recv(d.c_data.data(), d.c_data.size());
                tasks_child_v.push_back(d);
                break;
            }
            default:
                abort();
            }
        }
        dependence_v.resize(re.probe<task_dependence>());
        re.irecv(dependence_v.data(), dependence_v.size(), req);
    }

    task::task(): env(nullptr)
    { }

    task::~task()
    { }

    void task::set_environment(task_environment* e)
    { env = e; }

    void task::add_dependence(local_task_id parent, local_task_id child) const
    { env->add_dependence(parent, child); }

    size_t task::working_processes() const
    { return env->working_processes(); }

    void task::send(const sender& se) const
    { }

    void task::recv(const receiver& re)
    { }


    task_factory::performer_base::performer_base()
    { }

    task_factory::performer_base::~performer_base()
    { }

    std::vector<std::unique_ptr<task_factory::performer_base>>& task_factory::task_vec()
    {
        static std::vector<std::unique_ptr<performer_base>> v;
        return v;
    }

    task* task_factory::get(message_type id, const std::vector<message*>& info)
    { return dynamic_cast<task*>(message_init_factory::get(id, info)); }

    std::vector<message*> task_factory::get_info(message_type id)
    { return message_init_factory::get_info(id); }

    void task_factory::perform(perform_type id, task* t, const std::vector<message*>& args, const std::vector<const message*>& c_args)
    { task_vec().at(id)->perform(t, args, c_args); }

    const std::vector<bool>& task_factory::const_map(perform_type id)
    { return task_vec().at(id)->const_map(); }

}
