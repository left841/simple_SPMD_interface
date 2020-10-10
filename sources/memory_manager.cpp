#include "memory_manager.h"

namespace apl
{

    memory_manager::d_info& memory_manager::resolve_mes_id(message_id id)
    { return data_v[mes_map.find(id)->second]; }

    memory_manager::t_info& memory_manager::resolve_task_id(perform_id id)
    { return task_v[task_map.find(id)->second]; }

    size_t memory_manager::acquire_d_info()
    {
        if (first_empty_message != std::numeric_limits<size_t>::max())
        {
            size_t pos = first_empty_message;
            first_empty_message = data_v[first_empty_message].next_empty;
            return pos;
        }
        else
        {
            data_v.resize(data_v.size() + 1);
            return data_v.size() - 1;
        }
    }

    size_t memory_manager::acquire_t_info()
    {
        if (first_empty_task != std::numeric_limits<size_t>::max())
        {
            size_t pos = first_empty_task;
            first_empty_task = task_v[first_empty_task].next_empty;
            return pos;
        }
        else
        {
            task_v.resize(task_v.size() + 1);
            return task_v.size() - 1;
        }
    }

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
            message* m = nullptr;
            message_type type = MESSAGE_TYPE_UNDEFINED;
            std::vector<message*> info;
        };

        struct t_info
        {
            task* t = nullptr;
            perform_type type = PERFORM_TYPE_UNDEFINED;
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
            resolve_mes_id(id).created = false;
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
            resolve_task_id(id).parents_count = (*_tg.t_map.find(it->second.t)).second.parents.size();
        }
        tmpr.clear();

        for (auto iit = task_map.begin(); iit != task_map.end(); ++iit)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(dynamic_cast<task*>(resolve_mes_id(task_v[iit->second].m_id).d))).second.childs;
            task_v[iit->second].childs.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                task_v[iit->second].childs[j] = tmp[*it];
        }
        _tg.clear();
    }

    std::queue<perform_id> memory_manager::get_ready_tasks()
    {
        std::queue<perform_id> q;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            if (task_v[it->second].parents_count == 0)
                q.push(it->first);
        return q;
    }

    std::set<message_id>& memory_manager::get_unreferenced_messages()
    { return messages_to_del; }

    message_id memory_manager::add_message_init(message* ptr, message_type type, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        size_t id = acquire_d_info();
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true, 0, std::numeric_limits<size_t>::max()};
        mes_map[base_mes_id] = id;
        messages_to_del.insert(base_mes_id);
        return base_mes_id++;
    }

    message_id memory_manager::add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        size_t id = acquire_d_info();
        data_v[id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true, 0, std::numeric_limits<size_t>::max()};
        mes_map[base_mes_id] = id;
        messages_to_del.insert(base_mes_id);
        if (message_contained(parent))
        {
            inc_ref_count(parent);
            resolve_mes_id(parent).childs.push_back(base_mes_id);
        }
        return base_mes_id++;
    }

    perform_id memory_manager::add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        std::vector<perform_id> childs;
        size_t id = acquire_t_info();
        task_v[id] = {mes, type, PERFORM_ID_UNDEFINED, 0, 0, childs, data, const_data, std::numeric_limits<size_t>::max()};

        inc_ref_count(mes);
        for (message_id i: data)
            inc_ref_count(i);

        for (message_id i: const_data)
            inc_ref_count(i);

        task_map[base_task_id] = id;
        return base_task_id++;
    }

    task_id memory_manager::add_task(task* ptr, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info)
    {
        message_id m_id = add_message_init(ptr, type.mt, info);
        return {m_id, add_perform(m_id, type.pt, data, const_data)};
    }

    message_id memory_manager::create_message_init(message_type type, std::vector<message*>& info)
    {
        for (message* i: info)
            i->wait_requests();
        message* mes = message_init_factory::get(type, info);
        return add_message_init(mes, type, info);
    }

    message_id memory_manager::create_message_child(message_type type, message_id parent, std::vector<message*>& info)
    {
        message* mes;
        for (message* i: info)
            i->wait_requests();
        if (message_contained(parent))
            mes = message_child_factory::get(type, get_message(parent), info);
        else
            mes = message_child_factory::get(type, info);
        return add_message_child(mes, type, parent, info);
    }

    task_id memory_manager::create_task(task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info)
    {
        message_id m_id = create_message_init(type.mt, info);
        return {m_id, add_perform(m_id, type.pt, data, const_data)};
    }

    void memory_manager::include_child_to_parent(message_id child)
    {
        d_info& di = resolve_mes_id(child);
        message* parent = resolve_mes_id(di.parent).d;
        for (message* i: di.info)
            i->wait_requests();
        di.d->wait_requests();
        parent->wait_requests();
        message_child_factory::include(di.type, parent, di.d, di.info);
    }

    void memory_manager::include_child_to_parent_recursive(message_id child)
    {
        message_id parent;
        while ((parent = resolve_mes_id(child).parent) != MESSAGE_ID_UNDEFINED)
        {
            include_child_to_parent(child);
            child = parent;
        }
    }

    void memory_manager::add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        size_t ac_id = acquire_d_info();
        data_v[ac_id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::INIT, type, MESSAGE_ID_UNDEFINED, childs, true, 0, std::numeric_limits<size_t>::max()};
        mes_map[id] = ac_id;
        messages_to_del.insert(id);
    }

    void memory_manager::add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info)
    {
        std::vector<message_id> childs;
        size_t ac_id = acquire_d_info();
        data_v[ac_id] = {ptr, info, 0, MESSAGE_FACTORY_TYPE::CHILD, type, parent, childs, true, 0, std::numeric_limits<size_t>::max()};
        mes_map[id] = ac_id;
        messages_to_del.insert(id);
        if (message_contained(parent))
        {
            inc_ref_count(parent);
            resolve_mes_id(parent).childs.push_back(id);
        }
    }

    void memory_manager::add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        std::vector<perform_id> childs;
        size_t ac_id = acquire_t_info();
        task_v[ac_id] = {id.mi, type, PERFORM_ID_UNDEFINED, 0, 0, childs, data, const_data, std::numeric_limits<size_t>::max()};
        task_map[id.pi] = ac_id;
    }

    void memory_manager::add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info)
    {
        add_message_init_with_id(ptr, id.mi, type.mt, info);
        add_perform_with_id(id, type.pt, data, const_data);
    }

    void memory_manager::create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info)
    {
        for (message* i: info)
            i->wait_requests();
        message* mes = message_init_factory::get(type, info);
        return add_message_init_with_id(mes, id, type, info);
    }

    void memory_manager::create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info)
    {
        message* mes;
        for (message* i: info)
            i->wait_requests();
        if (message_contained(parent))
            mes = message_child_factory::get(type, get_message(parent), info);
        else
            mes = message_child_factory::get(type, info);
        return add_message_child_with_id(mes, id, type, parent, info);
    }

    void memory_manager::create_task_with_id(task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info)
    {
        create_message_init_with_id(id.mi, type.mt, info);
        add_perform_with_id(id, type.pt, data, const_data);
    }

    void memory_manager::update_message_versions(perform_id id)
    {
        t_info& ti = resolve_task_id(id);
        for (message_id i: ti.data)
            ++resolve_mes_id(i).version;
    }

    void memory_manager::update_version(message_id id, size_t new_version)
    { resolve_mes_id(id).version = new_version; }

    void memory_manager::add_dependence(perform_id parent, perform_id child)
    {
        resolve_task_id(parent).childs.push_back(child);
        ++resolve_task_id(child).parents_count;
    }

    void memory_manager::add_dependence(task_id parent, task_id child)
    { 
        resolve_task_id(parent.pi).childs.push_back(child.pi);
        ++resolve_task_id(child.pi).parents_count;
    }

    void memory_manager::perform_task(perform_id id, task_environment& te)
    {
        std::vector<message*> args;
        for (message_id i: get_perform_data(id))
            args.push_back(get_message(i));

        std::vector<const message*> c_args;
        for (message_id i: get_perform_const_data(id))
            c_args.push_back(get_message(i));

        t_info& ti = resolve_task_id(id);

        get_task(ti.m_id)->set_environment(&te);
        task_factory::perform(ti.type, get_task(ti.m_id), args, c_args);
    }

    void memory_manager::inc_ref_count(message_id id)
    {
        d_info& di = resolve_mes_id(id);
        if (di.refs_count == 0)
        {
            messages_to_del.erase(id);
            if ((di.parent != MESSAGE_ID_UNDEFINED) && message_contained(di.parent))
                inc_ref_count(di.parent);
        }
        ++di.refs_count;
    }

    void memory_manager::dec_ref_count(message_id id)
    {
        d_info& di = resolve_mes_id(id);
        --di.refs_count;
        if (di.refs_count == 0)
        {
            messages_to_del.insert(id);
            if ((di.parent != MESSAGE_ID_UNDEFINED) && message_contained(di.parent))
                dec_ref_count(di.parent);
        }
    }

    void memory_manager::delete_message(message_id id)
    {
        size_t data_v_id = mes_map.find(id)->second;
        d_info& di = data_v[data_v_id];
        if (di.created)
        {
            di.d->wait_requests();
            delete di.d;
            for (message* i: di.info)
                if (i != nullptr)
                {
                    i->wait_requests();
                    delete i;
                }
            di.created = false;
        }
        di.d = nullptr;
        di.info.clear();
        di.childs.clear();
        if (first_empty_message != std::numeric_limits<size_t>::max())
            di.next_empty = first_empty_message;
        first_empty_message = data_v_id;
        mes_map.erase(id);
        messages_to_del.erase(id);
    }

    void memory_manager::delete_perform(perform_id id)
    {
        size_t task_v_id = task_map.find(id)->second;
        t_info& ti = task_v[task_v_id];

        dec_ref_count(ti.m_id);
        for (message_id i: ti.data)
            dec_ref_count(i);

        for (message_id i: ti.const_data)
            dec_ref_count(i);

        ti.childs.clear();
        ti.data.clear();
        ti.const_data.clear();
        if (first_empty_task != std::numeric_limits<size_t>::max())
            ti.next_empty = first_empty_task;
        first_empty_task = task_v_id;
        task_map.erase(id);
    }

    void memory_manager::delete_task(task_id id)
    {
        delete_perform(id.pi);
        delete_message(id.mi);
    }

    void memory_manager::clear()
    {
        task_v.clear();
        while (mes_map.size())
            delete_message(mes_map.begin()->first);
        data_v.clear();
        mes_map.clear();
        task_map.clear();
        messages_to_del.clear();
        tasks_to_del.clear();
        base_mes_id = base_task_id = 0;
        first_empty_task = first_empty_message = std::numeric_limits<size_t>::max();
    }

    void memory_manager::set_message(message_id id, message* new_message)
    { resolve_mes_id(id).d = new_message; }

    void memory_manager::set_message_type(message_id id, message_type new_type)
    { resolve_mes_id(id).type = new_type; }

    void memory_manager::set_message_info(message_id id, std::vector<message*>& info)
    { resolve_mes_id(id).info = info; }

    void memory_manager::set_message_parent(message_id id, message_id new_parent)
    { resolve_mes_id(id).parent = new_parent; }

    void memory_manager::set_message_version(message_id id, size_t new_version)
    { resolve_mes_id(id).version = new_version; }

    void memory_manager::set_task(task_id id, task* new_task)
    { resolve_mes_id(id.mi).d = new_task; }

    void memory_manager::set_task_type(task_id id, task_type new_type)
    {
        resolve_mes_id(id.mi).type = new_type.mt;
        resolve_task_id(id.pi).type = new_type.pt;
    }

    void memory_manager::set_task_parent(task_id id, task_id new_parent)
    { resolve_task_id(id.pi).parent = new_parent.pi; }

    void memory_manager::set_perform_parents_count(perform_id id, size_t new_parents_count)
    { resolve_task_id(id).parents_count = new_parents_count; }

    void memory_manager::set_task_parents_count(task_id id, size_t new_parents_count)
    { resolve_task_id(id.pi).parents_count = new_parents_count; }

    void memory_manager::set_perform_created_childs(perform_id id, size_t new_created_childs)
    { resolve_task_id(id).created_childs = new_created_childs; }

    void memory_manager::set_task_created_childs(task_id id, size_t new_created_childs)
    { resolve_task_id(id.pi).created_childs = new_created_childs; }

    void memory_manager::set_task_childs(task_id id, std::vector<perform_id> new_childs)
    { resolve_task_id(id.pi).childs = new_childs; }

    void memory_manager::set_task_data(task_id id, std::vector<message_id> new_data)
    { resolve_task_id(id.pi).data = new_data; }

    void memory_manager::set_task_const_data(task_id id, std::vector<message_id> new_const_data)
    { resolve_task_id(id.pi).const_data = new_const_data; }

    size_t memory_manager::message_count()
    { return mes_map.size(); }

    message* memory_manager::get_message(message_id id)
    {
        d_info& di = resolve_mes_id(id);
        di.d->wait_requests();
        return di.d;
    }

    MESSAGE_FACTORY_TYPE memory_manager::get_message_factory_type(message_id id)
    { return resolve_mes_id(id).f_type; }

    message_type memory_manager::get_message_type(message_id id)
    { return resolve_mes_id(id).type; }

    std::vector<message*>& memory_manager::get_message_info(message_id id)
    { return resolve_mes_id(id).info; }

    message_id memory_manager::get_message_parent(message_id id)
    { return resolve_mes_id(id).parent; }

    size_t memory_manager::get_message_version(message_id id)
    { return resolve_mes_id(id).version; }

    size_t memory_manager::task_count()
    { return task_map.size(); }

    task_id memory_manager::get_task_id(perform_id id)
    { return {resolve_task_id(id).m_id, id}; }

    task* memory_manager::get_task(message_id id)
    { return dynamic_cast<task*>(get_message(id)); }

    task* memory_manager::get_task(task_id id)
    { return dynamic_cast<task*>(get_message(id.mi)); }

    perform_type memory_manager::get_perform_type(perform_id id)
    { return resolve_task_id(id).type; }

    task_type memory_manager::get_task_type(task_id id)
    { return {resolve_mes_id(id.mi).type, resolve_task_id(id.pi).type}; }

    perform_id memory_manager::get_perform_parent(perform_id id)
    { return resolve_task_id(id).parent; }

    task_id memory_manager::get_task_parent(task_id id)
    { return {resolve_task_id(resolve_task_id(id.pi).parent).m_id, resolve_task_id(id.pi).parent}; }

    size_t memory_manager::get_perform_parents_count(perform_id id)
    { return resolve_task_id(id).parents_count; }

    size_t memory_manager::get_task_parents_count(task_id id)
    { return resolve_task_id(id.pi).parents_count; }

    size_t memory_manager::get_perform_created_childs(perform_id id)
    { return resolve_task_id(id).created_childs; }

    size_t memory_manager::get_task_created_childs(task_id id)
    { return resolve_task_id(id.pi).created_childs; }

    std::vector<perform_id>& memory_manager::get_perform_childs(perform_id id)
    { return resolve_task_id(id).childs; }

    std::vector<perform_id>& memory_manager::get_task_childs(task_id id)
    { return resolve_task_id(id.pi).childs; }

    std::vector<message_id>& memory_manager::get_perform_data(perform_id id)
    { return resolve_task_id(id).data; }

    std::vector<message_id>& memory_manager::get_task_data(task_id id)
    { return resolve_task_id(id.pi).data; }

    std::vector<message_id>& memory_manager::get_perform_const_data(perform_id id)
    { return resolve_task_id(id).const_data; }

    std::vector<message_id>& memory_manager::get_task_const_data(task_id id)
    { return resolve_task_id(id.pi).const_data; }

    bool memory_manager::message_contained(message_id id)
    { return mes_map.find(id) != mes_map.end(); }

    bool memory_manager::message_has_parent(message_id id)
    { return resolve_mes_id(id).parent != MESSAGE_ID_UNDEFINED; }

    bool memory_manager::perform_has_parent(perform_id id)
    { return resolve_task_id(id).parent != PERFORM_ID_UNDEFINED; }

    bool memory_manager::task_has_parent(task_id id)
    { return resolve_task_id(id.pi).parent != PERFORM_ID_UNDEFINED; }

}
