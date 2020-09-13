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

        std::map<task*, size_t> tmp;
        std::map<message*, size_t> dmp;
        std::map<size_t, task*> tmpr;
        std::map<size_t, message*> dmpr;

        for (auto it = _tg.d_map.begin(); it != _tg.d_map.end(); ++it)
            dmpr[(*it).second.id] = (*it).first;

        for (auto it = dmpr.begin(); it != dmpr.end(); ++it)
        {
            message_id id = add_message((*it).second);
            dmp[(*it).second] = id;
        }
        dmpr.clear();

        for (auto it = _tg.t_map.begin(); it != _tg.t_map.end(); ++it)
            tmpr[(*it).second.id] = (*it).first;

        for (auto it = tmpr.begin(); it != tmpr.end(); ++it)
        {
            task_id id = add_task((*it).second);
            tmp[(*it).second] = id;
            task_v[id].parents_count = (*_tg.t_map.find((*it).second)).second.parents.size();
        }
        tmpr.clear();

        for (size_t i = 0; i < task_v.size(); ++i)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(task_v[i].t)).second.childs;
            task_v[i].childs.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                task_v[i].childs[j] = tmp[*it];
            for (j = 0; j < task_v[i].t->data.size(); ++j)
                task_v[i].data.push_back(dmp[task_v[i].t->data[j]]);
            for (j = 0; j < task_v[i].t->c_data.size(); ++j)
            {
                message* t = const_cast<message*>(task_v[i].t->c_data[j]);
                task_v[i].const_data.push_back(dmp[t]);
            }
        }
        _tg.clear();
    }

    std::queue<task_id> memory_manager::get_ready_tasks()
    {
        std::queue<task_id> q;
        for (task_id i = 0; i < task_v.size(); ++i)
            if ((task_v[i].t != nullptr) && (task_v[i].parents_count == 0))
                q.push(i);
        return q;
    }

    message_id memory_manager::add_message(message* ptr)
    { 
        std::vector<message_id> childs;
        std::vector<sendable*> info;
        data_v.push_back({ptr, info, 0, MESSAGE_FACTORY_TYPE::UNDEFINED, MESSAGE_TYPE_UNDEFINED, MESSAGE_ID_UNDEFINED, childs, false});
        return data_v.size() - 1;
    }

    task_id memory_manager::add_task(task* ptr)
    {
        std::vector<task_id> childs;
        std::vector<message_id> data;
        std::vector<message_id> const_data;
        task_v.push_back({ptr, TASK_TYPE_UNDEFINED, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data, false});
        return task_v.size() - 1;
    }

    message_id memory_manager::add_message_init(message* ptr, message_type type, std::vector<sendable*>& info)
    {
        std::vector<message_id> childs;
        data_v.push_back({ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true});
        return data_v.size() - 1;
    }

    message_id memory_manager::add_message_child(message* ptr, message_type type, message_id parent, std::vector<sendable*>& info)
    {
        std::vector<message_id> childs;
        data_v.push_back({ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true});
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
        return data_v.size() - 1;
    }

    task_id memory_manager::add_task(task* ptr, task_type type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        std::vector<task_id> childs;
        task_v.push_back({ptr, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data, true});
        return task_v.size() - 1;
    }

    message_id memory_manager::create_message_init(message_type type, std::vector<sendable*>& info)
    {
        message* mes = message_init_factory::get(type, info);
        std::vector<message_id> childs;
        data_v.push_back({mes, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true});
        return data_v.size() - 1;
    }

    message_id memory_manager::create_message_child(message_type type, message_id parent, std::vector<sendable*>& info)
    {
        message* mes;
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            mes = message_child_factory::get(type, *data_v[parent].d, info);
        else
            mes = message_child_factory::get(type, info);
        std::vector<message_id> childs;
        data_v.push_back({mes, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true});
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
        return data_v.size() - 1;
    }

    task_id memory_manager::create_task(task_type type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        std::vector<message*> mes_v;
        std::vector<const message*> const_mes_v;
        for (message_id i: data)
            mes_v.push_back(data_v[i].d);
        for (message_id i: const_data)
            const_mes_v.push_back(data_v[i].d);
        task* t = task_factory::get(type, mes_v, const_mes_v);
        std::vector<task_id> childs;
        task_v.push_back({t, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data, true});
        return task_v.size() - 1;
    }

    void memory_manager::include_child_to_parent(message_id child)
    {
        message_child_factory::include(data_v[child].type, *data_v[data_v[child].parent].d, *data_v[child].d, data_v[child].info);
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

    void memory_manager::add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<sendable*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true};
    }

    void memory_manager::add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<sendable*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true};
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
    }

    void memory_manager::add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        if (id >= task_v.size())
            task_v.resize(id + 1);
        std::vector<task_id> childs;
        task_v[id] = {ptr, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data, true};
    }

    void memory_manager::create_message_init_with_id(message_id id, message_type type, std::vector<sendable*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        message* mes = message_init_factory::get(type, info);
        std::vector<message_id> childs;
        data_v[id] = {mes, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true};
    }

    void memory_manager::create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<sendable*>& info)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        std::vector<message_id> childs;
        message* mes;
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            mes = message_child_factory::get(type, *data_v[parent].d, info);
        else
            mes = message_child_factory::get(type, info);
        data_v[id] = {mes, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true};
        if ((parent < data_v.size()) && (data_v[parent].d != nullptr))
            data_v[parent].childs.push_back(data_v.size() - 1);
    }

    void memory_manager::create_task_with_id(task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        if (id >= task_v.size())
            task_v.resize(id + 1);
        std::vector<message*> mes_v;
        std::vector<const message*> const_mes_v;
        for (message_id i: data)
            mes_v.push_back(data_v[i].d);
        for (message_id i : const_data)
            const_mes_v.push_back(data_v[i].d);
        task* t = task_factory::get(type, mes_v, const_mes_v);
        std::vector<task_id> childs;
        task_v[id] = {t, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data, true};
    }

    void memory_manager::update_message_versions(task_id id)
    {
        for (message_id i: task_v[id].data)
            ++data_v[i].version;
    }

    void memory_manager::update_version(message_id id, size_t new_version)
    { data_v[id].version = new_version; }

    void memory_manager::add_dependence(task_id parent, task_id child)
    { 
        task_v[parent].childs.push_back(child);
        ++task_v[child].parents_count;
    }

    void memory_manager::delete_message(message_id id)
    {
        if (data_v[id].created)
        {
            delete data_v[id].d;
            for (sendable* i: data_v[id].info)
                if (i != nullptr)
                    delete i;
            data_v[id].childs.clear();
            data_v[id].created = false;
        }
        data_v[id].d = nullptr;
        data_v[id].info.clear();
    }

    void memory_manager::delete_task(task_id id)
    {
        if (task_v[id].created)
            delete task_v[id].t;
        task_v[id].childs.clear();
        task_v[id].data.clear();
        task_v[id].const_data.clear();
        task_v[id].created = false;
    }

    void memory_manager::clear()
    {
        for (size_t i = 0; i < data_v.size(); ++i)
            delete_message(i);
        data_v.clear();
        for (size_t i = 0; i < task_v.size(); ++i)
            delete_task(i);
        task_v.clear();
    }

    void memory_manager::set_message(message_id id, message* new_message)
    { data_v[id].d = new_message; }

    void memory_manager::set_message_type(message_id id, message_type new_type)
    { data_v[id].type = new_type; }

    void memory_manager::set_message_info(message_id id, std::vector<sendable*>& info)
    { data_v[id].info = info; }

    void memory_manager::set_message_parent(message_id id, message_id new_parent)
    { data_v[id].parent = new_parent; }

    void memory_manager::set_message_version(message_id id, size_t new_version)
    { data_v[id].version = new_version; }

    void memory_manager::set_task(task_id id, task* new_task)
    { task_v[id].t = new_task; }

    void memory_manager::set_task_type(task_id id, task_type new_type)
    { task_v[id].type = new_type; }

    void memory_manager::set_task_parent(task_id id, task_id new_parent)
    { task_v[id].parent = new_parent; }

    void memory_manager::set_task_parents_count(task_id id, size_t new_parents_count)
    { task_v[id].parents_count = new_parents_count; }

    void memory_manager::set_task_created_childs(task_id id, size_t new_created_childs)
    { task_v[id].created_childs = new_created_childs; }

    void memory_manager::set_task_childs(task_id id, std::vector<task_id> new_childs)
    { task_v[id].childs = new_childs; }

    void memory_manager::set_task_data(task_id id, std::vector<message_id> new_data)
    { task_v[id].data = new_data; }

    void memory_manager::set_task_const_data(task_id id, std::vector<message_id> new_const_data)
    { task_v[id].const_data = new_const_data; }

    size_t memory_manager::message_count()
    { return data_v.size(); }

    message* memory_manager::get_message(message_id id)
    { return data_v[id].d; }

    MESSAGE_FACTORY_TYPE memory_manager::get_message_factory_type(message_id id)
    { return data_v[id].f_type; }

    message_type memory_manager::get_message_type(message_id id)
    { return data_v[id].type; }

    std::vector<sendable*>& memory_manager::get_message_info(message_id id)
    { return data_v[id].info; }

    message_id memory_manager::get_message_parent(message_id id)
    { return data_v[id].parent; }

    size_t memory_manager::get_message_version(message_id id)
    { return data_v[id].version; }

    size_t memory_manager::task_count()
    { return task_v.size(); }

    task* memory_manager::get_task(task_id id)
    { return task_v[id].t; }

    task_type memory_manager::get_task_type(task_id id)
    { return task_v[id].type; }

    task_id memory_manager::get_task_parent(task_id id)
    { return task_v[id].parent; }

    size_t memory_manager::get_task_parents_count(task_id id)
    { return task_v[id].parents_count; }

    size_t memory_manager::get_task_created_childs(task_id id)
    { return task_v[id].created_childs; }

    std::vector<task_id>& memory_manager::get_task_childs(task_id id)
    { return task_v[id].childs; }

    std::vector<message_id>& memory_manager::get_task_data(task_id id)
    { return task_v[id].data; }

    std::vector<message_id>& memory_manager::get_task_const_data(task_id id)
    { return task_v[id].const_data; }

    bool memory_manager::message_contained(message_id id)
    { return ((id) < data_v.size() && (data_v[id].d != nullptr)); }

    bool memory_manager::message_has_parent(message_id id)
    { return data_v[id].parent != MESSAGE_ID_UNDEFINED; }

    bool memory_manager::task_has_parent(task_id id)
    { return task_v[id].parent != TASK_ID_UNDEFINED; }

}
