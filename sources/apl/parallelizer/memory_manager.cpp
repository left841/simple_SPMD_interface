#include "apl/parallelizer/memory_manager.h"

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

        std::map<task*, task_id> tmp;
        std::map<message*, message_id> dmp;

        for (auto it = _tg.d_map.begin(); it != _tg.d_map.end(); ++it)
        {
            add_message_init_with_id(it->first, it->second.id, it->second.type, it->second.info);
            mes_map[it->second.id].c_type = CREATION_STATE::REFFERED;
            dmp[it->first] = it->second.id;
        }

        for (auto it = _tg.t_map.begin(); it != _tg.t_map.end(); ++it)
        {
            std::vector<message_id> data, c_data;
            for (size_t i = 0; i < task_factory::const_map(it->second.type).size(); ++i)
            {
                if (task_factory::const_map(it->second.type)[i])
                    c_data.push_back(dmp[it->second.data[i]]);
                else
                    data.push_back(dmp[it->second.data[i]]);
            }
            add_task_with_id(it->second.id, dmp[it->first], it->second.type, data, c_data);
            tmp[it->first] = it->second.id;
            task_map[it->second.id].parents_count = it->second.parents.size();
        }

        for (auto iit = task_map.begin(); iit != task_map.end(); ++iit)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(dynamic_cast<task*>(mes_map[iit->m_id].d))).second.childs;
            iit->childs.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                iit->childs[j] = tmp[*it];
        }
        _tg.clear();
    }

    std::queue<task_id> memory_manager::get_ready_tasks()
    {
        std::queue<task_id> q;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            if (it->parents_count == 0)
                q.push(it.key());
        return q;
    }

    std::set<message_id>& memory_manager::get_unreferenced_messages()
    { return messages_to_del; }

    std::set<message_id> memory_manager::get_messages_set()
    {
        std::set<message_id> s;
        for (auto it = mes_map.begin(); it != mes_map.end(); ++it)
            s.insert(it.key());
        return s;
    }

    std::set<task_id> memory_manager::get_tasks_set()
    {
        std::set<task_id> s;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            s.insert(it.key());
        return s;
    }

    message_id memory_manager::add_message_init(message* ptr, message_type type, std::vector<message*>& info)
    {
        std::set<message_id> childs;
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CREATED;
        message_id new_id = {base_mes_id++, parallel_engine::global_rank()};
        d_info& d = mes_map[new_id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        mes_graph[new_id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED};
        messages_to_del.insert(new_id);
        return new_id;
    }

    message_id memory_manager::add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info)
    {
        std::set<message_id> childs;
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CHILD;
        message_id new_id = {base_mes_id++, parallel_engine::global_rank()};
        d_info& d = mes_map[new_id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        mes_graph[new_id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED};
        messages_to_del.insert(new_id);
        if (mes_graph.contains(parent))
            insert_message_child(parent, new_id);
        return new_id;
    }

    task_id memory_manager::add_task(message_id mes, task_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        std::vector<task_id> childs;
        task_id new_id = {base_task_id++, parallel_engine::global_rank()};
        task_map[new_id] = {mes, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data};

        inc_ref_count(mes);
        for (message_id i: data)
            inc_ref_count(i);

        for (message_id i: const_data)
            inc_ref_count(i);

        return new_id;
    }

    message_id memory_manager::create_message_init(message_type type, std::vector<message*>& info)
    { return add_message_init(nullptr, type, info); }

    message_id memory_manager::create_message_child(message_type type, message_id parent, std::vector<message*>& info)
    { return add_message_child(nullptr, type, parent, info); }

    void memory_manager::add_message_to_graph(message_id id, message_type type)
    {
        std::set<message_id> childs;
        mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED};
        messages_to_del.insert(id);
    }

    void memory_manager::add_message_child_to_graph(message_id id, message_type type, message_id parent)
    {
        std::set<message_id> childs;
        mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED};
        messages_to_del.insert(id);
    }

    void memory_manager::include_child_to_parent(message_id child)
    {
        d_info& di = mes_map[child];
        message* parent = get_message(mes_graph[child].parent);
        di.info_req.wait_all();
        di.d_req.wait_all();
        message_child_factory::include(mes_graph[child].type, parent, di.d, di.info);
    }

    void memory_manager::include_child_to_parent_recursive(message_id child)
    {
        message_id parent;
        while (((parent = mes_graph[child].parent) != MESSAGE_ID_UNDEFINED) && message_contained(parent))
        {
            include_child_to_parent(child);
            child = parent;
        }
    }

    void memory_manager::add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info)
    {
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CREATED;
        d_info& d = mes_map[id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        if (!mes_graph.contains(id))
        {
            std::set<message_id> childs;
            mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED};
            messages_to_del.insert(id);
        }
    }

    void memory_manager::add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info)
    {
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CHILD;
        d_info& d = mes_map[id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        if (!mes_graph.contains(id))
        {
            std::set<message_id> childs;
            mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED};
            messages_to_del.insert(id);

            if (mes_graph.contains(parent))
                insert_message_child(parent, id);
        }
    }

    void memory_manager::add_task_with_id(task_id id, message_id base_id, task_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data)
    {
        std::vector<task_id> childs;
        task_map[id] = {base_id, type, TASK_ID_UNDEFINED, 0, 0, childs, data, const_data};

        inc_ref_count(base_id);
        for (message_id i: data)
            inc_ref_count(i);

        for (message_id i: const_data)
            inc_ref_count(i);
    }

    void memory_manager::create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info)
    { return add_message_init_with_id(nullptr, id, type, info); }

    void memory_manager::create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info)
    { return add_message_child_with_id(nullptr, id, type, parent, info); }

    void memory_manager::update_message_versions(task_id id)
    {
        t_info& ti = task_map[id];
        for (message_id i: ti.data)
            ++mes_map[i].version;
    }

    void memory_manager::update_version(message_id id, size_t new_version)
    { mes_map[id].version = new_version; }

    void memory_manager::add_dependence(task_id parent, task_id child)
    {
        task_map[parent].childs.push_back(child);
        ++task_map[child].parents_count;
    }

    void memory_manager::perform_task(task_id id, task_environment& te)
    {
        std::vector<message*> args;
        for (message_id i: get_task_data(id))
            args.push_back(get_message(i));

        std::vector<const message*> c_args;
        for (message_id i: get_task_const_data(id))
            c_args.push_back(get_message(i));

        t_info& ti = task_map[id];

        get_message_as_task(ti.m_id)->set_environment(&te);
        task_factory::perform(ti.type, get_message_as_task(ti.m_id), args, c_args);
    }

    void memory_manager::inc_ref_count(message_id id)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.erase(id);
        ++it.refs_count;
    }

    void memory_manager::dec_ref_count(message_id id)
    {
        auto& it = mes_graph[id];
        --it.refs_count;
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager::delete_message_from_graph(message_id id)
    {
        if (mes_graph.contains(id))
        {
            auto& it = mes_graph[id];
            if ((it.parent != MESSAGE_ID_UNDEFINED) && mes_graph.contains(it.parent))
                erase_message_child(it.parent, id);
            mes_graph.erase(id);
            messages_to_del.erase(id);
        }
    }

    void memory_manager::delete_message(message_id id)
    {
        if (mes_map.contains(id))
        {
            d_info& di = mes_map[id];
            if (di.c_type != CREATION_STATE::REFFERED)
            {
                if (di.c_type != CREATION_STATE::WAITING)
                {
                    di.d_req.wait_all();
                    delete di.d;
                }
                di.info_req.wait_all();
                for (message* i: di.info)
                    if (i != nullptr)
                        delete i;
                di.c_type = CREATION_STATE::UNDEFINED;
            }
            else
                di.d_req.wait_all();
            di.d = nullptr;
            di.info.clear();

            mes_map.erase(id);
        }
        delete_message_from_graph(id);
    }

    void memory_manager::delete_task(task_id id)
    {
        t_info& ti = task_map[id];

        dec_ref_count(ti.m_id);
        for (message_id i: ti.data)
            dec_ref_count(i);

        for (message_id i: ti.const_data)
            dec_ref_count(i);

        ti.childs.clear();
        ti.data.clear();
        ti.const_data.clear();
        task_map.erase(id);
    }

    void memory_manager::clear()
    {
        while (mes_map.size())
            delete_message(mes_map.begin().key());
        mes_map.clear();
        task_map.clear();
        mes_graph.clear();
        messages_to_del.clear();
        tasks_to_del.clear();
        base_mes_id = base_task_id = 0;
    }

    void memory_manager::set_message(message_id id, message* new_message)
    { mes_map[id].d = new_message; }

    void memory_manager::set_message_type(message_id id, message_type new_type)
    { mes_graph[id].type = new_type; }

    void memory_manager::set_message_info(message_id id, std::vector<message*>& info)
    { mes_map[id].info = info; }

    void memory_manager::set_message_parent(message_id id, message_id new_parent)
    { mes_graph[id].parent = new_parent; }

    void memory_manager::set_message_version(message_id id, size_t new_version)
    { mes_map[id].version = new_version; }

    void memory_manager::set_message_child_state(message_id id, CHILD_STATE st)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty() && (st == CHILD_STATE::NEWER))
            messages_to_del.erase(id);
        it.ch_state = st;
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager::insert_message_child(message_id id, message_id child)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.erase(id);
        it.childs.insert(child);
    }

    void memory_manager::erase_message_child(message_id id, message_id child)
    {
        auto& it = mes_graph[id];
        it.childs.erase(child);
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager::set_task_type(task_id id, task_type new_type)
    { task_map[id].type = new_type; }

    void memory_manager::set_task_parent(task_id id, task_id new_parent)
    { task_map[id].parent = new_parent; }

    void memory_manager::set_task_parents_count(task_id id, size_t new_parents_count)
    { task_map[id].parents_count = new_parents_count; }

    void memory_manager::set_task_created_childs(task_id id, size_t new_created_childs)
    { task_map[id].created_childs = new_created_childs; }

    void memory_manager::set_task_childs(task_id id, std::vector<task_id> new_childs)
    { task_map[id].childs = new_childs; }

    void memory_manager::set_task_data(task_id id, std::vector<message_id> new_data)
    { task_map[id].data = new_data; }

    void memory_manager::set_task_const_data(task_id id, std::vector<message_id> new_const_data)
    { task_map[id].const_data = new_const_data; }

    size_t memory_manager::message_count()
    { return mes_map.size(); }

    message* memory_manager::get_message(message_id id)
    {
        d_info& di = mes_map[id];
        if (di.c_type == CREATION_STATE::WAITING)
        {
            di.info_req.wait_all();
            if (mes_graph[id].f_type == MESSAGE_FACTORY_TYPE::INIT)
            {
                di.d = message_init_factory::get(mes_graph[id].type, di.info);
                di.c_type = CREATION_STATE::CREATED;
            }
            else
            {
                if (message_contained(mes_graph[id].parent))
                {
                    di.d = message_child_factory::get(mes_graph[id].type, get_message(mes_graph[id].parent), di.info);
                    di.c_type = CREATION_STATE::CHILD;
                }
                else
                {
                    di.d = message_child_factory::get(mes_graph[id].type, di.info);
                    di.c_type = CREATION_STATE::CREATED;
                }
            }
        }
        else
            di.d_req.wait_all();
        return di.d;
    }

    MESSAGE_FACTORY_TYPE memory_manager::get_message_factory_type(message_id id)
    { return mes_graph[id].f_type; }

    message_type memory_manager::get_message_type(message_id id)
    { return mes_graph[id].type; }

    std::vector<message*>& memory_manager::get_message_info(message_id id)
    {
        mes_map[id].info_req.wait_all();
        return mes_map[id].info;
    }

    message_id memory_manager::get_message_parent(message_id id)
    { return mes_graph[id].parent; }

    size_t memory_manager::get_message_version(message_id id)
    { return mes_map[id].version; }

    CHILD_STATE memory_manager::get_message_child_state(message_id id)
    { return mes_graph[id].ch_state; }

    std::set<message_id>& memory_manager::get_message_childs(message_id id)
    { return mes_graph[id].childs; }

    request_block& memory_manager::get_message_request_block(message_id id)
    { return mes_map[id].d_req; }

    request_block& memory_manager::get_message_info_request_block(message_id id)
    { return mes_map[id].info_req; }

    size_t memory_manager::task_count()
    { return task_map.size(); }

    message_id memory_manager::get_task_base_message(task_id id)
    { return task_map[id].m_id; }

    task* memory_manager::get_message_as_task(message_id id)
    { return dynamic_cast<task*>(get_message(id)); }

    task_type memory_manager::get_task_type(task_id id)
    { return task_map[id].type; }

    task_id memory_manager::get_task_parent(task_id id)
    { return task_map[id].parent; }

    size_t memory_manager::get_task_parents_count(task_id id)
    { return task_map[id].parents_count; }

    size_t memory_manager::get_task_created_childs(task_id id)
    { return task_map[id].created_childs; }

    std::vector<task_id>& memory_manager::get_task_childs(task_id id)
    { return task_map[id].childs; }

    std::vector<message_id>& memory_manager::get_task_data(task_id id)
    { return task_map[id].data; }

    std::vector<message_id>& memory_manager::get_task_const_data(task_id id)
    { return task_map[id].const_data; }

    bool memory_manager::message_contained(message_id id)
    { return mes_map.contains(id); }

    bool memory_manager::message_created(message_id id)
    { return mes_map[id].d != nullptr; }

    bool memory_manager::message_has_parent(message_id id)
    { return mes_graph[id].parent != MESSAGE_ID_UNDEFINED; }

    bool memory_manager::task_has_parent(task_id id)
    { return task_map[id].parent != TASK_ID_UNDEFINED; }

}
