#include "memory_manager.h"

namespace auto_parallel
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

        std::map<task*, int> tmp;
        std::map<message*, int> dmp;
        std::map<int, task*> tmpr;
        std::map<int, message*> dmpr;

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

    message_id memory_manager::add_message(message* ptr, size_t type)
    { 
        data_v.push_back({ptr, type, nullptr, nullptr, std::numeric_limits<size_t>::max(), 0, false});
        return data_v.size() - 1;
    }

    task_id memory_manager::add_task(task* ptr, size_t type)
    {
        std::vector<task_id> childs;
        std::vector<message_id> data;
        std::vector<message_id> const_data;
        task_v.push_back({ptr, type, std::numeric_limits<size_t>::max(), 0, 0, childs, data, const_data});
        return task_v.size() - 1;
    }

    //message_id memory_manager::create_message(size_t type)
    //{
    //    message* mes = message_factory::get(type);
    //    data_v.push_back({mes, type, nullptr, nullptr, std::numeric_limits<size_t>::max(), 0, true});
    //    return task_v.size() - 1;
    //}

    message_id memory_manager::create_message(size_t type, message::init_info_base* iib)
    {
        message* mes = message_factory::get(type, iib);
        data_v.push_back({mes, type, iib, nullptr, std::numeric_limits<size_t>::max(), 0, true});
        return data_v.size() - 1;
    }

    message_id memory_manager::create_message(size_t type, message_id parent, message::part_info_base* pib, message::init_info_base* iib)
    {
        message* mes = message_factory::get_part(type, data_v[parent].d, pib);
        data_v.push_back({mes, type, iib, pib, parent, 0, true});
        return data_v.size() - 1;
    }

    task_id memory_manager::create_task(size_t type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        std::vector<message*> mes_v;
        std::vector<const message*> const_mes_v;
        for (message_id i: data)
            mes_v.push_back(data_v[i].d);
        for (message_id i: const_data)
            const_mes_v.push_back(data_v[i].d);
        task* t = task_factory::get(type, mes_v, const_mes_v);
        std::vector<task_id> childs;
        task_v.push_back({t, type, std::numeric_limits<size_t>::max(), 0, 0, childs, data, const_data});
        return task_v.size() - 1;
    }

    //void memory_manager::create_message_with_id(message_id id, size_t type)
    //{
    //    if (id >= data_v.size())
    //        data_v.resize(id + 1);
    //    message* mes = message_factory::get(type);
    //    data_v[id] = {mes, type, nullptr, nullptr, std::numeric_limits<size_t>::max(), 0, true};
    //}

    void memory_manager::create_message_with_id(message_id id, size_t type, message::init_info_base* iib)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        message* mes = message_factory::get(type, iib);
        data_v[id] = {mes, type, iib, nullptr, std::numeric_limits<size_t>::max(), 0, true};
    }

    void memory_manager::create_message_with_id(message_id id, size_t type, message_id parent, message::part_info_base* pib, message::init_info_base* iib)
    {
        if (id >= data_v.size())
            data_v.resize(id + 1);
        message* mes = message_factory::get_part(type, data_v[parent].d, pib);
        data_v[id] = {mes, type, iib, pib, parent, 0, true};
    }

    void memory_manager::create_task_with_id(task_id id, size_t type, std::vector<message_id> data, std::vector<message_id> const_data)
    {
        if (id >= task_v.size())
            task_v.resize(id + 1);
        std::vector<message*> mes_v;
        std::vector<const message*> const_mes_v;
        for (message_id i : data)
            mes_v.push_back(data_v[i].d);
        for (message_id i : const_data)
            const_mes_v.push_back(data_v[i].d);
        task* t = task_factory::get(type, mes_v, const_mes_v);
        std::vector<task_id> childs;
        task_v[id] = {t, type, std::numeric_limits<size_t>::max(), 0, 0, childs, data, const_data};
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
            if (data_v[id].iib != nullptr)
                delete data_v[id].iib;
            if (data_v[id].pib != nullptr)
                delete data_v[id].pib;
            data_v[id].created = false;
        }
        //if (id == data_v.size() - 1)
            //data_v.pop_back();
        //else
        {
            data_v[id].d = nullptr;
            data_v[id].iib = nullptr;
            data_v[id].pib = nullptr;
        }
    }

    void memory_manager::delete_task(task_id id)
    {
        if (task_v[id].created)
            delete task_v[id].t;
        //if (id == task_v.size() - 1)
            //data_v.pop_back();
        //else
        {
            task_v[id].childs.clear();
            task_v[id].data.clear();
            task_v[id].const_data.clear();
            task_v[id].created = false;
        }
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

    void memory_manager::set_message_type(message_id id, size_t new_type)
    { data_v[id].type = new_type; }

    void memory_manager::set_message_init_info(message_id id, message::init_info_base* new_iib)
    { data_v[id].iib = new_iib; }

    void memory_manager::set_message_part_info(message_id id, message::part_info_base* new_pib)
    { data_v[id].pib = new_pib; }

    void memory_manager::set_message_parent(message_id id, size_t new_parent)
    { data_v[id].parent = new_parent; }

    void memory_manager::set_message_version(message_id id, size_t new_version)
    { data_v[id].version = new_version; }

    void memory_manager::set_task(task_id id, task* new_task)
    { task_v[id].t = new_task; }

    void memory_manager::set_task_type(task_id id, size_t new_type)
    { task_v[id].type = new_type; }

    void memory_manager::set_task_parent(task_id id, size_t new_parent)
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

    size_t memory_manager::get_message_type(message_id id)
    { return data_v[id].type; }

    message::init_info_base* memory_manager::get_message_init_info(message_id id)
    { return data_v[id].iib; }

    message::part_info_base* memory_manager::get_message_part_info(message_id id)
    { return data_v[id].pib; }

    size_t memory_manager::get_message_parent(message_id id)
    { return data_v[id].parent; }

    size_t memory_manager::get_message_version(message_id id)
    { return data_v[id].version; }

    size_t memory_manager::task_count()
    { return task_v.size(); }

    task* memory_manager::get_task(task_id id)
    { return task_v[id].t; }

    size_t memory_manager::get_task_type(task_id id)
    { return task_v[id].type; }

    size_t memory_manager::get_task_parent(task_id id)
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

    bool memory_manager::message_has_parent(message_id id)
    { return data_v[id].parent != std::numeric_limits<size_t>::max(); }

    bool memory_manager::task_has_parent(task_id id)
    { return task_v[id].parent != std::numeric_limits<size_t>::max(); }

}
