#include "memory_manager.h"

namespace apl
{

    memory_manager::memory_manager()
    { }

    memory_manager::memory_manager(task_graph& _tg)
    { init(_tg); }

    memory_manager::~memory_manager()
    { clear(); }

    void memory_manager::init(task_graph& _tg)
    {
        clear();

        struct m_info
        {
            message* m;
            message_type type;
            std::vector<message*> info;
        };

        struct t_info
        {
            task* t;
            perform_type type;
            std::vector<message*> data;
        };

        std::map<task*, perform_id> tmp;
        std::map<message*, message_id> dmp;
        std::map<size_t, t_info> tmpr;
        std::map<size_t, m_info> dmpr;

        for (auto it = _tg.d_map.begin(); it != _tg.d_map.end(); ++it)
            dmpr[it->second.id] = {it->first, it->second.type, it->second.info};

        for (auto it = dmpr.begin(); it != dmpr.end(); ++it)
        {
            message_id id = add_message_init(it->second.m, it->second.type, it->second.info);
            data_v[id].created = false;
            dmp[it->second.m] = id;
        }
        dmpr.clear();

        for (auto it = _tg.t_map.begin(); it != _tg.t_map.end(); ++it)
            tmpr[it->second.id] = {it->first, it->second.type, it->second.data};

        for (auto it = tmpr.begin(); it != tmpr.end(); ++it)
        {
            std::vector<message_id> data, c_data;
            for (size_t i = 0; i < task_factory::const_map(it->second.type).size(); ++i)
            {
                if (task_factory::const_map(it->second.type)[i])
                    c_data.push_back(dmp[it->second.data[i]]);
                else
                    data.push_back(dmp[it->second.data[i]]);
            }
            perform_id id = add_perform(dmp[it->second.t], it->second.type, data, c_data);
            tmp[it->second.t] = id;
            task_v[id].parents_count = (*_tg.t_map.find(it->second.t)).second.parents.size();
        }
        tmpr.clear();

        for (size_t i = 0; i < task_v.size(); ++i)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(dynamic_cast<task*>(data_v[task_v[i].m_id].d))).second.childs;
            task_v[i].childs.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                task_v[i].childs[j] = {task_v[tmp[*it]].m_id, tmp[*it]};
        }
        _tg.clear();
    }

    std::queue<perform_id> memory_manager::get_ready_tasks()
    {
        std::queue<perform_id> q;
        for (perform_id i = 0; i < task_v.size(); ++i)
            if (task_v[i].parents_count == 0)
                q.push(i);
        return q;
    }

    message_id memory_manager::add_message_init(message* ptr, message_type type, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        data_v.push_back({ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true});
        return data_v.size() - 1;
    }

    message_id memory_manager::add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        data_v.push_back({ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true});
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
        return data_v.size() - 1;
    }

    perform_id memory_manager::add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        std::vector<task_id> childs;
        task_v.push_back({mes, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data});
        return task_v.size() - 1;
    }

    task_id memory_manager::add_task(task* ptr, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info)
    {
        std::vector<task_id> childs;
        message_id m_id = add_message_init(ptr, type.mt, info);
        task_v.push_back({m_id, type.pt, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data});
        return {m_id, task_v.size() - 1};
    }

    message_id memory_manager::create_message_init(message_type type, std::vector<message*>& info)
    {
        for (message* i: info)
            i->wait_requests();
        message* mes = message_init_factory::get(type, info);
        std::vector<message_id> childs;
        data_v.push_back({mes, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true});
        return data_v.size() - 1;
    }

    message_id memory_manager::create_message_child(message_type type, message_id parent, std::vector<message*>& info)
    {
        message* mes;
        for (message* i: info)
            i->wait_requests();
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            mes = message_child_factory::get(type, data_v[parent].d, info);
        else
            mes = message_child_factory::get(type, info);
        std::vector<message_id> childs;
        data_v.push_back({mes, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true});
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
        return data_v.size() - 1;
    }

    task_id memory_manager::create_task(task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info)
    {
        message_id m_id = create_message_init(type.mt, info);
        std::vector<task_id> childs;
        task_v.push_back({m_id, type.pt, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data});
        return {m_id, task_v.size() - 1};
    }

    void memory_manager::include_child_to_parent(message_id child)
    {
        message_child_factory::include(data_v[child].type, data_v[data_v[child].parent].d, data_v[child].d, data_v[child].info);
    }

    void memory_manager::include_child_to_parent_recursive(message_id child)
    {
        message_id id = child;
        while (data_v[child].parent != MESSAGE_ID_UNDEFINED)
        {
            data_v[data_v[child].parent].d->wait_requests();
            include_child_to_parent(child);
            child = data_v[child].parent;
        }
    }

    void memory_manager::add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true};
    }

    void memory_manager::add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true};
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
    }

    void memory_manager::add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        if (id.pi >= task_v.size())
            task_v.resize(id.pi + 1);
        std::vector<task_id> childs;
        task_v[id.pi] = {id.mi, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data};
    }

    void memory_manager::add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info)
    {
        add_message_init_with_id(ptr, id.mi, type.mt, info);
        if (id.pi >= task_v.size())
            task_v.resize(id.pi + 1);
        std::vector<task_id> childs;
        task_v[id.pi] = {id.mi, type.pt, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data};
    }

    void memory_manager::create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        for (message* i: info)
            i->wait_requests();
        message* mes = message_init_factory::get(type, info);
        std::vector<message_id> childs;
        data_v[id] = {mes, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true};
    }

    void memory_manager::create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        message* mes;
        for (message* i: info)
            i->wait_requests();
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            mes = message_child_factory::get(type, data_v[parent].d, info);
        else
            mes = message_child_factory::get(type, info);
        data_v[id] = {mes, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true};
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
    }

    void memory_manager::create_task_with_id(task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info)
    {
        create_message_init_with_id(id.mi, type.mt, info);
        if (id.pi >= task_v.size())
            task_v.resize(id.pi + 1);
        std::vector<task_id> childs;
        task_v[id.pi] = {id.mi, type.pt, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data};
    }

    void memory_manager::update_message_versions(message_id id)
    {
        for (message_id i: task_v[id].data)
            ++data_v[i].version;
    }

    void memory_manager::update_version(message_id id, size_t new_version)
    { data_v[id].version = new_version; }

    void memory_manager::add_dependence(perform_id parent, perform_id child)
    {
        task_v[parent].childs.push_back(get_task_id(child));
        ++task_v[child].parents_count;
    }

    void memory_manager::add_dependence(task_id parent, task_id child)
    { 
        task_v[parent.pi].childs.push_back(child);
        ++task_v[child.pi].parents_count;
    }

    void memory_manager::perform_task(perform_id id, task_environment& te)
    {
        for (message_id j: get_perform_data(id))
            get_message(j)->wait_requests();

        for (message_id j: get_perform_const_data(id))
            get_message(j)->wait_requests();

        get_message(task_v[id].m_id)->wait_requests();

        std::vector<message*> args;
        for (message_id i: get_perform_data(id))
            args.push_back(get_message(i));

        std::vector<const message*> c_args;
        for (message_id i: get_perform_const_data(id))
            c_args.push_back(get_message(i));

        get_task({task_v[id].m_id, id})->set_environment(&te);
        task_factory::perform(task_v[id].type, get_task({task_v[id].m_id, id}), args, c_args);
    }

    void memory_manager::delete_message(message_id id)
    {
        if (data_v[id].created)
        {
            data_v[id].d->wait_requests();
            delete data_v[id].d;
            for (message* i: data_v[id].info)
                if (i != nullptr)
                {
                    i->wait_requests();
                    delete i;
                }
            data_v[id].childs.clear();
            data_v[id].created = false;
        }
        data_v[id].d = nullptr;
        data_v[id].info.clear();
    }

    void memory_manager::delete_task(task_id id)
    {
        delete_message(id.mi);
        task_v[id.pi].childs.clear();
        task_v[id.pi].data.clear();
        task_v[id.pi].const_data.clear();
    }

    void memory_manager::clear()
    {
        task_v.clear();
        for (size_t i = 0; i < data_v.size(); ++i)
            delete_message(i);
        data_v.clear();
    }

    void memory_manager::set_message(message_id id, message* new_message)
    { data_v[id].d = new_message; }

    void memory_manager::set_message_type(message_id id, message_type new_type)
    { data_v[id].type = new_type; }

    void memory_manager::set_message_info(message_id id, std::vector<message*>& info)
    { data_v[id].info = info; }

    void memory_manager::set_message_parent(message_id id, message_id new_parent)
    { data_v[id].parent = new_parent; }

    void memory_manager::set_message_version(message_id id, size_t new_version)
    { data_v[id].version = new_version; }

    void memory_manager::set_task(task_id id, task* new_task)
    { data_v[id.mi].d = new_task; }

    void memory_manager::set_task_type(task_id id, task_type new_type)
    {
        data_v[id.mi].type = new_type.mt;
        task_v[id.pi].type = new_type.pt;
    }

    void memory_manager::set_task_parent(task_id id, task_id new_parent)
    { task_v[id.pi].parent = new_parent; }

    void memory_manager::set_task_parents_count(task_id id, size_t new_parents_count)
    { task_v[id.pi].parents_count = new_parents_count; }

    void memory_manager::set_perform_created_childs(perform_id id, size_t new_created_childs)
    { task_v[id].created_childs = new_created_childs; }

    void memory_manager::set_task_created_childs(task_id id, size_t new_created_childs)
    { task_v[id.pi].created_childs = new_created_childs; }

    void memory_manager::set_task_childs(task_id id, std::vector<task_id> new_childs)
    { task_v[id.pi].childs = new_childs; }

    void memory_manager::set_task_data(task_id id, std::vector<message_id> new_data)
    { task_v[id.pi].data = new_data; }

    void memory_manager::set_task_const_data(task_id id, std::vector<message_id> new_const_data)
    { task_v[id.pi].const_data = new_const_data; }

    size_t memory_manager::message_count()
    { return data_v.size(); }

    message* memory_manager::get_message(message_id id)
    { return data_v[id].d; }

    MESSAGE_FACTORY_TYPE memory_manager::get_message_factory_type(message_id id)
    { return data_v[id].f_type; }

    message_type memory_manager::get_message_type(message_id id)
    { return data_v[id].type; }

    std::vector<message*>& memory_manager::get_message_info(message_id id)
    { return data_v[id].info; }

    message_id memory_manager::get_message_parent(message_id id)
    { return data_v[id].parent; }

    size_t memory_manager::get_message_version(message_id id)
    { return data_v[id].version; }

    size_t memory_manager::task_count()
    { return task_v.size(); }

    task_id memory_manager::get_task_id(perform_id id)
    { return {task_v[id].m_id, id}; }

    task* memory_manager::get_task(task_id id)
    { return dynamic_cast<task*>(data_v[id.mi].d); }

    perform_type memory_manager::get_perform_type(perform_id id)
    { return task_v[id].type; }

    task_type memory_manager::get_task_type(task_id id)
    { return {data_v[id.mi].type, task_v[id.pi].type}; }

    perform_id memory_manager::get_perform_parent(perform_id id)
    { return task_v[id].parent.pi; }

    task_id memory_manager::get_task_parent(task_id id)
    { return task_v[id.pi].parent; }

    size_t memory_manager::get_task_parents_count(task_id id)
    { return task_v[id.pi].parents_count; }

    size_t memory_manager::get_perform_created_childs(perform_id id)
    { return task_v[id].created_childs; }

    size_t memory_manager::get_task_created_childs(task_id id)
    { return task_v[id.pi].created_childs; }

    std::vector<task_id>& memory_manager::get_perform_childs(perform_id id)
    { return task_v[id].childs; }

    std::vector<task_id>& memory_manager::get_task_childs(task_id id)
    { return task_v[id.pi].childs; }

    std::vector<message_id>& memory_manager::get_perform_data(perform_id id)
    { return task_v[id].data; }

    std::vector<message_id>& memory_manager::get_task_data(task_id id)
    { return task_v[id.pi].data; }

    std::vector<message_id>& memory_manager::get_perform_const_data(perform_id id)
    { return task_v[id].const_data; }

    std::vector<message_id>& memory_manager::get_task_const_data(task_id id)
    { return task_v[id.pi].const_data; }

    bool memory_manager::message_contained(message_id id)
    { return ((id) < data_v.size() && (data_v[id].d != nullptr)); }

    bool memory_manager::message_has_parent(message_id id)
    { return data_v[id].parent != MESSAGE_ID_UNDEFINED; }

    bool memory_manager::perform_has_parent(perform_id id)
    { return (task_v[id].parent.mi != TASK_ID_UNDEFINED.mi) || (task_v[id].parent.pi != TASK_ID_UNDEFINED.pi); }

    bool memory_manager::task_has_parent(task_id id)
    { return (task_v[id.pi].parent.mi != TASK_ID_UNDEFINED.mi) || (task_v[id.pi].parent.pi != TASK_ID_UNDEFINED.pi); }

}
