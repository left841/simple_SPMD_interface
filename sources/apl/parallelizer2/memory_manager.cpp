#include "apl/parallelizer2/memory_manager.h"

namespace apl
{

    memory_manager2::memory_manager2()
    { }

    memory_manager2::memory_manager2(task_graph& _tg, process main_proc)
    { init(_tg, main_proc); }

    memory_manager2::~memory_manager2()
    { clear(); }

    void memory_manager2::init(task_graph& _tg, process main_proc)
    {
        clear();

        std::map<task*, perform_id> tmp;
        std::map<message*, message_id> dmp;

        for (auto it = _tg.d_map.begin(); it != _tg.d_map.end(); ++it)
        {
            add_message_init_with_id(it->first, it->second.id, it->second.type, it->second.info, main_proc);
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
            add_perform_with_id({dmp[it->first], it->second.id}, it->second.type, data, c_data, main_proc);
            tmp[it->first] = it->second.id;
            task_map[it->second.id].parents_count = it->second.parents.size();
        }

        for (auto iit = task_map.begin(); iit != task_map.end(); ++iit)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(dynamic_cast<task*>(mes_map[iit->m_id].d))).second.childs;
            iit->childs_v.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                iit->childs_v[j] = tmp[*it];
        }
        _tg.clear();
    }

    std::deque<perform_id> memory_manager2::get_ready_tasks(process owner)
    {
        std::deque<perform_id> q;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            if ((it->parents_count == 0) && (it->owner == owner))
                q.push_back(it.key());
        return q;
    }

    std::deque<perform_id> memory_manager2::get_ready_tasks()
    {
        std::deque<perform_id> q;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            if (it->parents_count == 0)
                q.push_back(it.key());
        return q;
    }

    std::set<message_id>& memory_manager2::get_unreferenced_messages()
    { return messages_to_del; }

    std::set<message_id> memory_manager2::get_messages_set()
    {
        std::set<message_id> s;
        for (auto it = mes_map.begin(); it != mes_map.end(); ++it)
            s.insert(it.key());
        return s;
    }

    std::set<perform_id> memory_manager2::get_performs_set()
    {
        std::set<perform_id> s;
        for (auto it = task_map.begin(); it != task_map.end(); ++it)
            s.insert(it.key());
        return s;
    }

    message_id memory_manager2::add_message_init(message* ptr, message_type type, std::vector<message*>& info, process owner)
    {
        std::set<message_id> childs;
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CREATED;
        message_id new_id = {base_mes_id++, parallel_engine::global_rank()};
        memory_graph_node& d = mes_map[new_id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        mes_graph[new_id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED, owner};
        messages_to_del.insert(new_id);
        return new_id;
    }

    message_id memory_manager2::add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info, process owner)
    {
        std::set<message_id> childs;
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CHILD;
        message_id new_id = {base_mes_id++, parallel_engine::global_rank()};
        memory_graph_node& d = mes_map[new_id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        mes_graph[new_id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED, owner};
        messages_to_del.insert(new_id);
        if (mes_graph.contains(parent))
            insert_message_child(parent, new_id);
        return new_id;
    }

    perform_id memory_manager2::add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data, process owner)
    {
        std::vector<perform_id> childs;
        perform_id new_id = {base_task_id++, parallel_engine::global_rank()};
        task_map[new_id] = {mes, type, PERFORM_ID_UNDEFINED, 0, 0, childs, data, const_data, owner};

        inc_ref_count(mes);
        for (message_id i: data)
            inc_ref_count(i);

        for (message_id i: const_data)
            inc_ref_count(i);

        return new_id;
    }

    task_id memory_manager2::add_task(task* ptr, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner)
    {
        message_id m_id = add_message_init(ptr, type.mt, info, owner);
        return {m_id, add_perform(m_id, type.pt, data, const_data, owner)};
    }

    message_id memory_manager2::create_message_init(message_type type, std::vector<message*>& info, process owner)
    { return add_message_init(nullptr, type, info, owner); }

    message_id memory_manager2::create_message_child(message_type type, message_id parent, std::vector<message*>& info, process owner)
    { return add_message_child(nullptr, type, parent, info, owner); }

    task_id memory_manager2::create_task(task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner)
    {
        message_id m_id = create_message_init(type.mt, info, owner);
        return {m_id, add_perform(m_id, type.pt, data, const_data, owner)};
    }

    void memory_manager2::add_message_to_graph(message_id id, message_type type, process owner)
    {
        std::set<message_id> childs;
        mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED, owner};
        messages_to_del.insert(id);
    }

    void memory_manager2::add_message_child_to_graph(message_id id, message_type type, message_id parent, process owner)
    {
        std::set<message_id> childs;
        mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED, owner};
        messages_to_del.insert(id);
    }

    void memory_manager2::include_child_to_parent(message_id child)
    {
        memory_graph_node& di = mes_map[child];
        message* parent = get_message(mes_graph[child].parent);
        di.info_req.wait_all();
        di.d_req.wait_all();
        message_child_factory::include(mes_graph[child].type, parent, di.d, di.info);
    }

    void memory_manager2::include_child_to_parent_recursive(message_id child)
    {
        message_id parent;
        while (((parent = mes_graph[child].parent) != MESSAGE_ID_UNDEFINED) && message_contained(parent))
        {
            include_child_to_parent(child);
            child = parent;
        }
    }

    void memory_manager2::add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info, process owner)
    {
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CREATED;
        memory_graph_node& d = mes_map[id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        if (!mes_graph.contains(id))
        {
            std::set<message_id> childs;
            mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::INIT, MESSAGE_ID_UNDEFINED, childs, 0, CHILD_STATE::UNDEFINED, owner};
            messages_to_del.insert(id);
        }
    }

    void memory_manager2::add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info, process owner)
    {
        CREATION_STATE state = (ptr == nullptr) ? CREATION_STATE::WAITING: CREATION_STATE::CHILD;
        memory_graph_node& d = mes_map[id];
        d.d = ptr;
        d.info = info;
        d.version = 0;
        d.c_type = state;
        if (!mes_graph.contains(id))
        {
            std::set<message_id> childs;
            mes_graph[id] = {type, MESSAGE_FACTORY_TYPE::CHILD, parent, childs, 0, CHILD_STATE::INCLUDED, owner};
            messages_to_del.insert(id);

            if (mes_graph.contains(parent))
                insert_message_child(parent, id);
        }
    }

    void memory_manager2::add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data, process owner)
    {
        std::vector<perform_id> childs;
        task_map[id.pi] = {id.mi, type, PERFORM_ID_UNDEFINED, 0, 0, childs, data, const_data, owner};

        inc_ref_count(id.mi);
        for (message_id i: data)
            inc_ref_count(i);

        for (message_id i: const_data)
            inc_ref_count(i);
    }

    void memory_manager2::add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner)
    {
        add_message_init_with_id(ptr, id.mi, type.mt, info, owner);
        add_perform_with_id(id, type.pt, data, const_data, owner);
    }

    void memory_manager2::create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info, process owner)
    { return add_message_init_with_id(nullptr, id, type, info, owner); }

    void memory_manager2::create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info, process owner)
    { return add_message_child_with_id(nullptr, id, type, parent, info, owner); }

    void memory_manager2::create_task_with_id(task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner)
    {
        create_message_init_with_id(id.mi, type.mt, info, owner);
        add_perform_with_id(id, type.pt, data, const_data, owner);
    }

    void memory_manager2::update_message_versions(perform_id id)
    {
        task_graph_node& ti = task_map[id];
        for (message_id i: ti.data)
            ++mes_map[i].version;
    }

    void memory_manager2::update_version(message_id id, size_t new_version)
    { mes_map[id].version = new_version; }

    void memory_manager2::add_dependence(perform_id parent, perform_id child)
    {
        task_map[parent].childs_v.push_back(child);
        ++task_map[child].parents_count;
    }

    void memory_manager2::add_dependence(task_id parent, task_id child)
    { 
        task_map[parent.pi].childs_v.push_back(child.pi);
        ++task_map[child.pi].parents_count;
    }

    void memory_manager2::send_message(message_id id, const intracomm& comm, process proc)
    { comm.send<message>(get_message(id), proc); }

    void memory_manager2::recv_message(message_id id, const intracomm& comm, process proc)
    { comm.recv<message>(get_message(id), proc); }

    void memory_manager2::perform_task(perform_id id, task_environment& te)
    {
        std::vector<message*> args;
        for (message_id i: get_perform_data(id))
            args.push_back(get_message(i));

        std::vector<const message*> c_args;
        for (message_id i: get_perform_const_data(id))
            c_args.push_back(get_message(i));

        task_graph_node& ti = task_map[id];

        get_task(ti.m_id)->set_environment(&te);
        task_factory::perform(ti.type, get_task(ti.m_id), args, c_args);
    }

    void memory_manager2::inc_ref_count(message_id id)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.erase(id);
        ++it.refs_count;
    }

    void memory_manager2::dec_ref_count(message_id id)
    {
        auto& it = mes_graph[id];
        --it.refs_count;
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager2::delete_message_from_graph(message_id id)
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

    void memory_manager2::delete_message(message_id id)
    {
        if (mes_map.contains(id))
        {
            memory_graph_node& di = mes_map[id];
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

    void memory_manager2::delete_perform(perform_id id)
    {
        task_graph_node& ti = task_map[id];

        dec_ref_count(ti.m_id);
        for (message_id i: ti.data)
            dec_ref_count(i);

        for (message_id i: ti.const_data)
            dec_ref_count(i);

        ti.childs_v.clear();
        ti.data.clear();
        ti.const_data.clear();
        task_map.erase(id);
    }

    void memory_manager2::delete_task(task_id id)
    {
        delete_perform(id.pi);
        delete_message(id.mi);
    }

    void memory_manager2::clear()
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

    void memory_manager2::set_message(message_id id, message* new_message)
    { mes_map[id].d = new_message; }

    void memory_manager2::set_message_type(message_id id, message_type new_type)
    { mes_graph[id].type = new_type; }

    void memory_manager2::set_message_info(message_id id, std::vector<message*>& info)
    { mes_map[id].info = info; }

    void memory_manager2::set_message_parent(message_id id, message_id new_parent)
    { mes_graph[id].parent = new_parent; }

    void memory_manager2::set_message_version(message_id id, size_t new_version)
    { mes_map[id].version = new_version; }

    void memory_manager2::set_message_child_state(message_id id, CHILD_STATE st)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty() && (st == CHILD_STATE::NEWER))
            messages_to_del.erase(id);
        it.ch_state = st;
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager2::insert_message_child(message_id id, message_id child)
    {
        auto& it = mes_graph[id];
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.erase(id);
        it.childs.insert(child);
    }

    void memory_manager2::erase_message_child(message_id id, message_id child)
    {
        auto& it = mes_graph[id];
        it.childs.erase(child);
        if ((it.refs_count == 0) && (it.ch_state != CHILD_STATE::NEWER) && it.childs.empty())
            messages_to_del.insert(id);
    }

    void memory_manager2::set_task(task_id id, task* new_task)
    { mes_map[id.mi].d = new_task; }

    void memory_manager2::set_task_type(task_id id, task_type new_type)
    {
        mes_graph[id.mi].type = new_type.mt;
        task_map[id.pi].type = new_type.pt;
    }

    void memory_manager2::set_task_parent(task_id id, task_id new_parent)
    { task_map[id.pi].parent = new_parent.pi; }

    void memory_manager2::set_perform_parents_count(perform_id id, size_t new_parents_count)
    { task_map[id].parents_count = new_parents_count; }

    void memory_manager2::set_task_parents_count(task_id id, size_t new_parents_count)
    { task_map[id.pi].parents_count = new_parents_count; }

    void memory_manager2::set_perform_created_childs(perform_id id, size_t new_created_childs)
    { task_map[id].created_childs_count = new_created_childs; }

    void memory_manager2::set_task_created_childs(task_id id, size_t new_created_childs)
    { task_map[id.pi].created_childs_count = new_created_childs; }

    void memory_manager2::set_task_childs(task_id id, std::vector<perform_id> new_childs)
    { task_map[id.pi].childs_v = new_childs; }

    void memory_manager2::set_task_data(task_id id, std::vector<message_id> new_data)
    { task_map[id.pi].data = new_data; }

    void memory_manager2::set_task_const_data(task_id id, std::vector<message_id> new_const_data)
    { task_map[id.pi].const_data = new_const_data; }

    size_t memory_manager2::ins_count() const
    {
        return ins.size();
    }

    size_t memory_manager2::outs_count() const
    {
        return outs.size();
    }

    size_t memory_manager2::child_ins_count() const
    {
        return child_ins.size();
    }

    size_t memory_manager2::child_outs_count() const
    {
        return child_outs.size();
    }

    void memory_manager2::connect_extern_in(perform_id out, perform_id in, size_t ext_proc)
    {
        auto it = ins.find({out, in, ext_proc});
        if (it != ins.end())
        {
            auto jt = task_map.find(it->in);
            if (jt != task_map.end())
                --jt->parents_count;
            ins.erase(it);
        }
    }

    void memory_manager2::connect_extern_child_out(perform_id out, perform_id in, size_t ext_proc)
    {
        auto it = child_outs.find({out, in, ext_proc});
        if (it != child_outs.end())
        {
            auto jt = task_map.find(it->out);
            if (jt != task_map.end())
                --jt->created_childs_count;
            child_outs.erase(it);
        }
    }

    process memory_manager2::find_connection_out_by_in_out(perform_id out, perform_id in) const
    {
        auto it = outs.lower_bound({out, in, 0});
        if ((it != outs.end()) && (it->out == out) && (it->in == in))
            return it->external_group_id;
        return MPI_PROC_NULL;
    }

    process memory_manager2::find_connection_child_in_by_in_out(perform_id out, perform_id in) const
    {
        auto it = child_ins.lower_bound({out, in, 0});
        if ((it != child_ins.end()) && (it->out == out) && (it->in == in))
            return it->external_group_id;
        return MPI_PROC_NULL;
    }

    void memory_manager2::erase_out(perform_id out, perform_id in, size_t ext_proc)
    { outs.erase({out, in, ext_proc}); }

    void memory_manager2::erase_child_in(perform_id out, perform_id in, size_t ext_proc)
    { child_ins.erase({out, in, ext_proc}); }

    void memory_manager2::insert_out(perform_id out, perform_id in, size_t ext_proc)
    {
        outs.insert({out, in, ext_proc});
    }

    void memory_manager2::insert_task_graph_node(perform_id id, const task_graph_node& tgn)
    {
        task_map[id] = tgn;
    }

    void memory_manager2::erase_subgraph(const sub_graph& graph, size_t new_owner)
    {
        for (perform_id i: graph.tasks)
            task_map.erase(i);
        //for (const graph_connection& i: graph.ins)
        //    if (i.external_group_id == parallel_engine::global_rank())
        //        outs.insert({i.out, i.in, new_owner});
        //for (const graph_connection& i: graph.outs)
        //    if (i.external_group_id == parallel_engine::global_rank())
        //        ins.insert({i.out, i.in, new_owner});
        //for (const graph_connection& i: graph.child_ins)
        //    if (i.external_group_id == parallel_engine::global_rank())
        //        child_outs.insert({i.out, i.in, new_owner});
        //for (const graph_connection& i: graph.child_outs)
        //    if (i.external_group_id == parallel_engine::global_rank())
        //        child_ins.insert({i.out, i.in, new_owner});
    }

    void memory_manager2::send_task_node(perform_id id, const intracomm& comm, process proc)
    {
        auto it = task_map.find(id);
        comm.send(&(*it), proc);
    }

    void memory_manager2::recv_task_node(perform_id id, const intracomm& comm, process proc)
    {
        task_graph_node tgn;
        comm.recv(&tgn, proc);
        task_map.insert({id, tgn});
    }

    size_t memory_manager2::message_count()
    { return mes_map.size(); }

    message* memory_manager2::get_message(message_id id)
    {
        memory_graph_node& di = mes_map[id];
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

    MESSAGE_FACTORY_TYPE memory_manager2::get_message_factory_type(message_id id)
    { return mes_graph[id].f_type; }

    message_type memory_manager2::get_message_type(message_id id)
    { return mes_graph[id].type; }

    std::vector<message*>& memory_manager2::get_message_info(message_id id)
    {
        mes_map[id].info_req.wait_all();
        return mes_map[id].info;
    }

    message_id memory_manager2::get_message_parent(message_id id)
    { return mes_graph[id].parent; }

    size_t memory_manager2::get_message_version(message_id id)
    { return mes_map[id].version; }

    CHILD_STATE memory_manager2::get_message_child_state(message_id id)
    { return mes_graph[id].ch_state; }

    std::set<message_id>& memory_manager2::get_message_childs(message_id id)
    { return mes_graph[id].childs; }

    request_block& memory_manager2::get_message_request_block(message_id id)
    { return mes_map[id].d_req; }

    request_block& memory_manager2::get_message_info_request_block(message_id id)
    { return mes_map[id].info_req; }

    size_t memory_manager2::task_count()
    { return task_map.size(); }

    task_id memory_manager2::get_task_id(perform_id id)
    { return { task_map[id].m_id, id}; }

    task* memory_manager2::get_task(message_id id)
    { return dynamic_cast<task*>(get_message(id)); }

    task* memory_manager2::get_task(task_id id)
    { return dynamic_cast<task*>(get_message(id.mi)); }

    perform_type memory_manager2::get_perform_type(perform_id id)
    { return task_map[id].type; }

    task_type memory_manager2::get_task_type(task_id id)
    { return {mes_graph[id.mi].type, task_map[id.pi].type}; }

    perform_id memory_manager2::get_perform_parent(perform_id id)
    { return task_map[id].parent; }

    task_id memory_manager2::get_task_parent(task_id id)
    { return {task_map[task_map[id.pi].parent].m_id, task_map[id.pi].parent}; }

    size_t memory_manager2::get_perform_parents_count(perform_id id)
    { return task_map[id].parents_count; }

    size_t memory_manager2::get_task_parents_count(task_id id)
    { return task_map[id.pi].parents_count; }

    size_t memory_manager2::get_perform_created_childs(perform_id id)
    { return task_map[id].created_childs_count; }

    size_t memory_manager2::get_task_created_childs(task_id id)
    { return task_map[id.pi].created_childs_count; }

    std::vector<perform_id>& memory_manager2::get_perform_childs(perform_id id)
    { return task_map[id].childs_v; }

    std::vector<perform_id>& memory_manager2::get_task_childs(task_id id)
    { return task_map[id.pi].childs_v; }

    std::vector<message_id>& memory_manager2::get_perform_data(perform_id id)
    { return task_map[id].data; }

    std::vector<message_id>& memory_manager2::get_task_data(task_id id)
    { return task_map[id.pi].data; }

    std::vector<message_id>& memory_manager2::get_perform_const_data(perform_id id)
    { return task_map[id].const_data; }

    std::vector<message_id>& memory_manager2::get_task_const_data(task_id id)
    { return task_map[id.pi].const_data; }

    bool memory_manager2::message_contained(message_id id) const
    { return mes_map.contains(id); }

    bool memory_manager2::perform_contained(perform_id id) const
    { return task_map.contains(id); }

    bool memory_manager2::message_created(message_id id)
    { return mes_map[id].d != nullptr; }

    bool memory_manager2::message_has_parent(message_id id)
    { return mes_graph[id].parent != MESSAGE_ID_UNDEFINED; }

    bool memory_manager2::perform_has_parent(perform_id id)
    { return task_map[id].parent != PERFORM_ID_UNDEFINED; }

    bool memory_manager2::task_has_parent(task_id id)
    { return task_map[id.pi].parent != PERFORM_ID_UNDEFINED; }

    memory_manager_graph_adapter::memory_manager_graph_adapter(memory_manager2& mem): graph_adapter()
    { }

    message_graph_node& memory_manager_graph_adapter::get_message_node(message_id id) const
    {
        return message_graph_node();
    }

    task_graph_node& memory_manager_graph_adapter::get_task_node(perform_id id) const
    {
        return task_graph_node();
    }

}
