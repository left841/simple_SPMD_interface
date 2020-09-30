#include "parallelizer.h"

namespace apl
{

    const int parallelizer::main_proc = 0;

    parallelizer::parallelizer(): comm(MPI_COMM_WORLD), instr_comm(comm)
    { }

    parallelizer::parallelizer(task_graph& _tg): comm(MPI_COMM_WORLD), instr_comm(comm), memory(_tg), ready_tasks(memory.get_ready_tasks())
    { }

    parallelizer::~parallelizer()
    { }

    void parallelizer::init(task_graph& _tg)
    {
        memory.init(_tg);
        ready_tasks = std::move(memory.get_ready_tasks());
    }

    void parallelizer::execution()
    {
        if (comm.rank() == main_proc)
            master();
        else
            worker();

        clear();
    }

    void parallelizer::execution(task_graph& _tg)
    {
        init(_tg);
        execution();
    }

    void parallelizer::master()
    {
        std::vector<std::set<message_id>> versions(comm.size());
        for (std::set<message_id>& i: versions)
            for (size_t j = 0; j < memory.message_count(); ++j)
                i.insert(j);

        std::vector<std::set<message_id>> contained(comm.size());
        for (process i = 0; i < comm.size(); ++i)
            for (message_id j = 0; j < memory.message_count(); ++j)
                contained[i].insert(j);

        std::vector<std::set<perform_id>> contained_tasks(comm.size());
        for (process i = 0; i < comm.size(); ++i)
            for (perform_id j = 0; j < memory.task_count(); ++j)
                contained_tasks[i].insert(j);

        std::vector<instruction> ins(comm.size());
        std::vector<std::vector<perform_id>> assigned(comm.size());
        size_t all_assigned = 0;

        while (ready_tasks.size())
        {
            size_t sub = ready_tasks.size() / comm.size();
            size_t per = ready_tasks.size() % comm.size();

            size_t px = sub + ((per != 0) ? 1: 0);
            for (size_t j = 0; j < px; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
                ++all_assigned;
            }

            for (process i = 1; i < comm.size(); ++i)
            {
                px = sub + ((i < per) ? 1: 0);
                for (size_t j = 0; j < px; ++j)
                {
                    send_task_data(ready_tasks.front(), i, ins[i], versions, contained);
                    assigned[i].push_back(ready_tasks.front());
                    ready_tasks.pop();
                    ++all_assigned;
                }
            }

            for (process i = 1; i < comm.size(); ++i)
                for (perform_id j: assigned[i])
                    assign_task(memory.get_task_id(j), i, ins[i], contained_tasks);

            for (process i = 1; i < comm.size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            while (all_assigned > 0)
            {
                if (assigned[0].size() > 0)
                {
                    perform_id i = assigned[0].back();
                    std::vector<local_message_id> data, c_data;
                    for (message_id j: memory.get_perform_data(i))
                        data.push_back({j, MESSAGE_SOURCE::TASK_ARG});
                    for (message_id j: memory.get_perform_const_data(i))
                        c_data.push_back({j, MESSAGE_SOURCE::TASK_ARG_C});
                    task_data td = {memory.get_perform_type(i), data, c_data};
                    task_environment te(td, i);
                    te.set_proc_count(comm.size());

                    memory.perform_task(i, te);
                    end_main_task(i, te, versions, contained, contained_tasks);
                    assigned[0].pop_back();
                    --all_assigned;
                }
                for (process i = 1; i < comm.size(); ++i)
                {
                    if (assigned[i].size() > 0)
                    {
                        wait_task(i, versions, contained, contained_tasks);
                        assigned[i].pop_back();
                        --all_assigned;
                    }
                }
            }
        }

        instruction end;
        end.add_end();
        for (int i = 1; i < instr_comm.size(); ++i)
            instr_comm.send(&end, i);
    }

    void parallelizer::send_task_data(perform_id tid, process proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
    {
        for (message_id i: memory.get_perform_data(tid))
        {
            if (con[proc].find(i) == con[proc].end())
            {
                if (memory.get_message_factory_type(i) == MESSAGE_FACTORY_TYPE::CHILD)
                {
                    ins.add_message_part_creation(i, memory.get_message_type(i), memory.get_message_parent(i));
                    if ((con[proc].find(memory.get_message_parent(i)) == con[proc].end()) || (ver[proc].find(memory.get_message_parent(i)) == ver[proc].end()))
                        ins.add_message_receiving(i);
                }
                else
                {
                    ins.add_message_creation(i, memory.get_message_type(i));
                    ins.add_message_receiving(i);
                }
                con[proc].insert(i);
                ver[proc].insert(i);
            }
            else if (ver[proc].find(i) == ver[proc].end())
            {
                ins.add_message_receiving(i);
                ver[proc].insert(i);
            }
        }

        for (message_id i: memory.get_perform_const_data(tid))
        {
            if (con[proc].find(i) == con[proc].end())
            {
                if (memory.get_message_factory_type(i) == MESSAGE_FACTORY_TYPE::CHILD)
                {
                    ins.add_message_part_creation(i, memory.get_message_type(i), memory.get_message_parent(i));
                    if ((con[proc].find(memory.get_message_parent(i)) == con[proc].end()) || (ver[proc].find(memory.get_message_parent(i)) == ver[proc].end()))
                        ins.add_message_receiving(i);
                }
                else
                {
                    ins.add_message_creation(i, memory.get_message_type(i));
                    ins.add_message_receiving(i);
                }
                con[proc].insert(i);
                ver[proc].insert(i);
            }
            else if (ver[proc].find(i) == ver[proc].end())
            {
                ins.add_message_receiving(i);
                ver[proc].insert(i);
            }
        }

        message_id mid = memory.get_task_id(tid).mi;
        if (con[proc].find(mid) == con[proc].end())
        {
            ins.add_message_creation(mid, memory.get_message_type(mid));
            ins.add_message_receiving(mid);
            con[proc].insert(mid);
            ver[proc].insert(mid);
        }
        else if (ver[proc].find(mid) == ver[proc].end())
        {
            ins.add_message_receiving(mid);
            ver[proc].insert(mid);
        }
    }

    void parallelizer::assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<perform_id>>& com)
    {
        if (com[proc].find(tid.pi) == com[proc].end())
        {
            ins.add_task_creation(tid, memory.get_task_type(tid), memory.get_task_data(tid), memory.get_task_const_data(tid));
            com[proc].insert(tid.pi);
        }
        ins.add_task_execution(tid);
    }

    void parallelizer::send_instruction(process proc, instruction& ins)
    {
        instr_comm.send(&ins, proc);

        for (const instruction_block& i: ins)
        {
            switch (i.command())
            {
            case INSTRUCTION::MES_RECV:
            {
                const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                comm.send(memory.get_message(j.id()), proc);
                break;
            }
            case INSTRUCTION::MES_CREATE:
            {
                const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                for (message* p: memory.get_message_info(j.id()))
                    instr_comm.send(p, proc);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                for (message* p: memory.get_message_info(j.id()))
                    instr_comm.send(p, proc);
                break;
            }
            case INSTRUCTION::TASK_CREATE:

                break;

            case INSTRUCTION::TASK_EXE:

                break;

            default:
                comm.abort(555);
            }
        }
    }

    void parallelizer::end_main_task(perform_id tid, task_environment& env, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<perform_id>>& con_t)
    {
        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_perform_data(tid))
        {
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
            memory.update_version(i, memory.get_message_version(i) + 1);
        }

        message_id t_mes_id = memory.get_task_id(tid).mi;

        for (process k = 0; k < comm.size(); ++k)
            ver[k].erase(t_mes_id);
        ver[main_proc].insert(t_mes_id);
        memory.update_version(t_mes_id, memory.get_message_version(t_mes_id) + 1);

        for (message_id i: memory.get_perform_data(tid))
        {
            memory.get_message(i)->wait_requests();
            memory.include_child_to_parent_recursive(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                for (process k = 1; k < comm.size(); ++k)
                    ver[k].erase(memory.get_message_parent(j));
                j = memory.get_message_parent(j);
            }
        }

        for (const local_message_id& i: env.result_message_ids())
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data& d = env.created_messages_init()[i.id];
                    messages_init_id.push_back(memory.create_message_init(d.type, d.ii));
                    con[main_proc].insert(messages_init_id.back());
                    ver[main_proc].insert(messages_init_id.back());
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data& d = env.added_messages_init()[i.id];
                    messages_init_add_id.push_back(memory.add_message_init(d.mes, d.type, d.ii));
                    con[main_proc].insert(messages_init_add_id.back());
                    ver[main_proc].insert(messages_init_add_id.back());
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    message_child_data& d = env.created_messages_child()[i.id];
                    message_id src;
                    switch (d.sourse.src)
                    {
                        case MESSAGE_SOURCE::TASK_ARG:
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = d.sourse.id;
                            break;
                        }
                        case MESSAGE_SOURCE::INIT:
                        {
                            src = messages_init_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::INIT_A:
                        {
                            src = messages_init_add_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD:
                        {
                            src = messages_childs_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD_A:
                        {
                            src = messages_childs_add_id[d.sourse.id];
                            break;
                        }
                    }
                    memory.get_message(src)->wait_requests();
                    messages_childs_id.push_back(memory.create_message_child(d.type, src, d.pi));
                    con[main_proc].insert(messages_childs_id.back());
                    ver[main_proc].insert(messages_childs_id.back());
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    message_child_add_data& d = env.added_messages_child()[i.id];
                    message_id src;
                    switch (d.sourse.src)
                    {
                        case MESSAGE_SOURCE::TASK_ARG:
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = d.sourse.id;
                            break;
                        }
                        case MESSAGE_SOURCE::INIT:
                        {
                            src = messages_init_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::INIT_A:
                        {
                            src = messages_init_add_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD:
                        {
                            src = messages_childs_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD_A:
                        {
                            src = messages_childs_add_id[d.sourse.id];
                            break;
                        }
                    }
                    for (process k = 1; k < comm.size(); ++k)
                        ver[k].erase(src);
                    memory.get_message(src)->wait_requests();
                    messages_childs_add_id.push_back(memory.add_message_child(d.mes, d.type, src, d.pi));
                    con[main_proc].insert(messages_childs_add_id.back());
                    ver[main_proc].insert(messages_childs_add_id.back());
                    break;
                }
            }
        }

        size_t tid_childs = 0;
        for (const local_task_id& i: env.result_task_ids())
        {
            message_id mes_t_id;
            std::vector<local_message_id> local_data, local_c_data;
            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
                    break;
                }
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = i.mes.id;
                    break;
                }
                case MESSAGE_SOURCE::INIT:
                {
                    mes_t_id = messages_init_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    mes_t_id = messages_init_add_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    mes_t_id = messages_childs_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    mes_t_id = messages_childs_add_id[i.mes.id];
                    break;
                }
            }

            std::vector<message_id> data_id;
            data_id.reserve(local_data.size());
            for (local_message_id k: local_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(k.id);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT:
                    {
                        data_id.push_back(messages_init_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT_A:
                    {
                        data_id.push_back(messages_init_add_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD:
                    {
                        data_id.push_back(messages_childs_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        data_id.push_back(messages_childs_add_id[k.id]);
                        break;
                    }
                }
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(local_c_data.size());
            for (local_message_id k: local_c_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(k.id);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT:
                    {
                        const_data_id.push_back(messages_init_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT_A:
                    {
                        const_data_id.push_back(messages_init_add_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD:
                    {
                        const_data_id.push_back(messages_childs_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        const_data_id.push_back(messages_childs_add_id[k.id]);
                        break;
                    }
                }
            }

            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    perform_id id = memory.add_perform(mes_t_id, t.type, data_id, const_data_id);
                    tasks_id.push_back({mes_t_id, id});
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    perform_id id = memory.add_perform(mes_t_id, t.type, data_id, const_data_id);
                    tasks_child_id.push_back({mes_t_id, id});
                    memory.set_task_parent({mes_t_id, id}, memory.get_task_id(tid));
                    con_t[main_proc].insert(id);
                    break;
                }
            }
        }
        memory.set_perform_created_childs(tid, memory.get_perform_created_childs(tid) + tid_childs);

        for (const task_dependence& i: env.created_dependences())
        {
            perform_id parent;
            perform_id child;
            switch (i.parent.src)
            {
                case TASK_SOURCE::INIT:
                {
                    parent = tasks_id[i.parent.id].pi;
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    parent = tasks_child_id[i.parent.id].pi;
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    parent = i.parent.id;
                    break;
                }
            }
            switch (i.child.src)
            {
                case TASK_SOURCE::INIT:
                {
                    child = tasks_id[i.child.id].pi;
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    child = tasks_child_id[i.child.id].pi;
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    child = i.child.id;
                    break;
                }
            }
            memory.add_dependence(parent, child);
        }

        for (task_id i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i.pi);
        }

        for (task_id i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i.pi);
        }

        update_ready_tasks(tid);
    }

    void parallelizer::wait_task(process proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<perform_id>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv(&res_ins, proc);
        instruction::const_iterator it = res_ins.begin();
        const instruction_block& ins = *it;
        if (ins.command() != INSTRUCTION::TASK_RES)
            comm.abort(111);
        const instruction_task_result& result = dynamic_cast<const instruction_task_result&>(ins);
        
        task_id tid = result.id();
        task_environment env(tid.pi);
        res_ins.clear();

        comm.recv(&env, proc);
        env.wait_requests();

        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_task_data(tid))
        {
            comm.recv(memory.get_message(i), proc);
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
            ver[proc].insert(i);
            memory.update_version(i, memory.get_message_version(i) + 1);
        }

        comm.recv(memory.get_message(tid.mi), proc);
        for (process k = 0; k < comm.size(); ++k)
            ver[k].erase(tid.mi);
        ver[main_proc].insert(tid.mi);
        ver[proc].insert(tid.mi);
        memory.update_version(tid.mi, memory.get_message_version(tid.mi) + 1);

        for (message_id i: memory.get_task_data(tid))
        {
            memory.get_message(i)->wait_requests();
            memory.include_child_to_parent_recursive(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                for (process k = 1; k < comm.size(); ++k)
                    ver[k].erase(memory.get_message_parent(j));
                j = memory.get_message_parent(j);
            }
        }

        for (const local_message_id& i: env.result_message_ids())
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data& d = env.created_messages_init()[i.id];
                    for (message* p: d.ii)
                        p->wait_requests();
                    messages_init_id.push_back(memory.create_message_init(d.type, d.ii));
                    con[main_proc].insert(messages_init_id.back());
                    ver[main_proc].insert(messages_init_id.back());
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data& d = env.added_messages_init()[i.id];
                    for (message* p: d.ii)
                        p->wait_requests();
                    messages_init_add_id.push_back(memory.create_message_init(d.type, d.ii));
                    comm.recv(memory.get_message(messages_init_add_id.back()), proc);
                    con[main_proc].insert(messages_init_add_id.back());
                    ver[main_proc].insert(messages_init_add_id.back());
                    con[proc].insert(messages_init_add_id.back());
                    ver[proc].insert(messages_init_add_id.back());
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    message_child_data& d = env.created_messages_child()[i.id];
                    message_id src;
                    switch (d.sourse.src)
                    {
                        case MESSAGE_SOURCE::TASK_ARG:
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = d.sourse.id;
                            break;
                        }
                        case MESSAGE_SOURCE::INIT:
                        {
                            src = messages_init_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::INIT_A:
                        {
                            src = messages_init_add_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD:
                        {
                            src = messages_childs_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD_A:
                        {
                            src = messages_childs_add_id[d.sourse.id];
                            break;
                        }
                        default:
                            comm.abort(765);
                    }
                    memory.get_message(src)->wait_requests();
                    for (message* p: d.pi)
                        p->wait_requests();
                    messages_childs_id.push_back(memory.create_message_child(d.type, src, d.pi));
                    con[main_proc].insert(messages_childs_id.back());
                    ver[main_proc].insert(messages_childs_id.back());
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    message_child_add_data& d = env.added_messages_child()[i.id];
                    message_id src;
                    switch (d.sourse.src)
                    {
                        case MESSAGE_SOURCE::TASK_ARG:
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = d.sourse.id;
                            break;
                        }
                        case MESSAGE_SOURCE::INIT:
                        {
                            src = messages_init_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::INIT_A:
                        {
                            src = messages_init_add_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD:
                        {
                            src = messages_childs_id[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::CHILD_A:
                        {
                            src = messages_childs_add_id[d.sourse.id];
                            break;
                        }
                        default:
                            comm.abort(765);
                    }
                    for (process k = 1; k < comm.size(); ++k)
                        ver[k].erase(src);
                    memory.get_message(src)->wait_requests();
                    for (message* p: d.pi)
                        p->wait_requests();
                    messages_childs_add_id.push_back(memory.create_message_child(d.type, src, d.pi));
                    comm.recv(memory.get_message(messages_childs_add_id.back()), proc);
                    con[main_proc].insert(messages_childs_add_id.back());
                    ver[main_proc].insert(messages_childs_add_id.back());
                    con[proc].insert(messages_childs_add_id.back());
                    ver[proc].insert(messages_childs_add_id.back());
                    break;
                }
                default:
                    comm.abort(765);
            }
        }
        size_t tid_childs = 0;
        for (const local_task_id& i: env.result_task_ids())
        {
            message_id mes_t_id;
            std::vector<local_message_id> local_data, local_c_data;
            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
                    break;
                }
                default:
                    comm.abort(765);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = i.mes.id;
                    break;
                }
                case MESSAGE_SOURCE::INIT:
                {
                    mes_t_id = messages_init_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    mes_t_id = messages_init_add_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    mes_t_id = messages_childs_id[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    mes_t_id = messages_childs_add_id[i.mes.id];
                    break;
                }
            }

            std::vector<message_id> data_id;
            data_id.reserve(local_data.size());
            for (local_message_id k: local_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(k.id);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT:
                    {
                        data_id.push_back(messages_init_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT_A:
                    {
                        data_id.push_back(messages_init_add_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD:
                    {
                        data_id.push_back(messages_childs_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        data_id.push_back(messages_childs_add_id[k.id]);
                        break;
                    }
                    default:
                        comm.abort(765);
                }
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(local_c_data.size());
            for (local_message_id k: local_c_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(k.id);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT:
                    {
                        const_data_id.push_back(messages_init_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::INIT_A:
                    {
                        const_data_id.push_back(messages_init_add_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD:
                    {
                        const_data_id.push_back(messages_childs_id[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        const_data_id.push_back(messages_childs_add_id[k.id]);
                        break;
                    }
                    default:
                        comm.abort(765);
                }
            }

            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    perform_id id = memory.add_perform(mes_t_id, t.type, data_id, const_data_id);
                    tasks_id.push_back({mes_t_id, id});
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    perform_id id = memory.add_perform(mes_t_id, t.type, data_id, const_data_id);
                    tasks_child_id.push_back({mes_t_id, id});
                    memory.set_task_parent({mes_t_id, id}, tid);
                    break;
                }
                default:
                    comm.abort(765);
            }
        }
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + tid_childs);

        for (const task_dependence& i: env.created_dependences())
        {
            task_id parent;
            task_id child;

            switch (i.parent.src)
            {
                case TASK_SOURCE::INIT:
                {
                    parent = tasks_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    parent = tasks_child_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    parent = memory.get_task_id(i.parent.id);
                    break;
                }
                default:
                    comm.abort(765);
            }
            switch (i.child.src)
            {
                case TASK_SOURCE::INIT:
                {
                    child = tasks_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    child = tasks_child_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    child = memory.get_task_id(i.child.id);
                    break;
                }
                default:
                    comm.abort(765);
            }

            memory.add_dependence(parent, child);
        }

        if (env.added_messages_child().size() + env.added_messages_init().size() > 0)
        {
            res_ins.add_add_result_to_memory(messages_init_add_id, messages_childs_add_id);
            instr_comm.send(&res_ins, proc);
        }

        for (task_id i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i.pi);
        }

        for (task_id i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i.pi);
        }

        update_ready_tasks(tid.pi);
    }

    void parallelizer::update_ready_tasks(perform_id tid)
    {
        perform_id c_t = tid;
        while (1)
        {
            if (memory.get_perform_created_childs(c_t) == 0)
                for (task_id i: memory.get_perform_childs(c_t))
                {
                    memory.set_task_parents_count(i, memory.get_task_parents_count(i) - 1);
                    if (memory.get_task_parents_count(i) == 0)
                        ready_tasks.push(i.pi);
                }
            else
                break;
            if (!memory.perform_has_parent(c_t))
                break;
            else
            {
                c_t = memory.get_perform_parent(c_t);
                memory.set_perform_created_childs(c_t, memory.get_perform_created_childs(c_t) - 1);
            }
        }
    }

    void parallelizer::worker()
    {
        instruction cur_inst;
        while(1)
        {
            instr_comm.recv(&cur_inst, main_proc);

            for (const instruction_block& i: cur_inst)
            {
                switch (i.command())
                {
                case INSTRUCTION::MES_SEND:
                {
                    const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                    comm.send(memory.get_message(j.id()), main_proc);
                    break;
                }
                case INSTRUCTION::MES_RECV:
                {
                    const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                    comm.recv(memory.get_message(j.id()), main_proc);
                    break;
                }
                case INSTRUCTION::MES_CREATE:
                {
                    const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                    std::vector<message*> iib = message_init_factory::get_info(j.type());
                    for (message* p: iib)
                        instr_comm.recv(p, main_proc);
                    for (message* p: iib)
                        p->wait_requests();
                    memory.create_message_init_with_id(j.id(), j.type(), iib);
                    break;
                }
                case INSTRUCTION::MES_P_CREATE:
                {
                    const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                    std::vector<message*> pib = message_child_factory::get_info(j.type());
                    for (message* p: pib)
                        instr_comm.recv(p, main_proc);
                    for (message* p: pib)
                        p->wait_requests();
                    if (memory.message_contained(j.source()))
                        memory.get_message(j.source())->wait_requests();
                    memory.create_message_child_with_id(j.id(), j.type(), j.source(), pib);
                    break;
                }
                case INSTRUCTION::TASK_CREATE:
                {
                    const instruction_task_create& j = dynamic_cast<const instruction_task_create&>(i);
                    memory.add_perform_with_id(j.id(), j.type().pt, j.data(), j.const_data());
                    break;
                }
                case INSTRUCTION::TASK_EXE:
                {
                    const instruction_task_execute& j = dynamic_cast<const instruction_task_execute&>(i);
                    execute_task(j.id().pi);
                    break;
                }
                case INSTRUCTION::END:

                    goto end;

                default:

                    instr_comm.abort(234);
                }
            }
            cur_inst.clear();
        }
        end:;
    }

    void parallelizer::execute_task(perform_id id)
    {
        std::vector<local_message_id> data, c_data;
        for (message_id i: memory.get_perform_data(id))
            data.push_back({i, MESSAGE_SOURCE::TASK_ARG});
        for (message_id i: memory.get_perform_const_data(id))
            c_data.push_back({i, MESSAGE_SOURCE::TASK_ARG_C});

        task_data td = {memory.get_perform_type(id), data, c_data};
        task_environment env(std::move(td), id);
        env.set_proc_count(comm.size());

        memory.perform_task(id, env);

        instruction res;
        res.add_task_result(memory.get_task_id(id));

        instr_comm.send(&res, main_proc);

        std::vector<local_message_id> result_message_ids(env.result_message_ids());
        std::vector<message_init_add_data> added_messages_init(env.added_messages_init());
        std::vector<message_child_add_data> added_messages_child(env.added_messages_child());

        comm.send(&env, main_proc);

        for (message_id i: memory.get_perform_data(id))
            comm.send(memory.get_message(i), main_proc);

        comm.send(memory.get_message(memory.get_task_id(id).mi), main_proc);

        for (const local_message_id& i: result_message_ids)
        {
            if (i.src == MESSAGE_SOURCE::INIT_A)
                comm.send(added_messages_init[i.id].mes, main_proc);
            else if (i.src == MESSAGE_SOURCE::CHILD_A)
                comm.send(added_messages_child[i.id].mes, main_proc);
        }
        env.wait_requests();

        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_add_id;

        if (env.added_messages_child().size() + env.added_messages_init().size() > 0)
        {
            instruction add_ins;
            instr_comm.recv(&add_ins, main_proc);

            instruction::const_iterator it = add_ins.begin();

            if ((*it).command() != INSTRUCTION::ADD_RES_TO_MEMORY)
                comm.abort(532);

            const instruction_add_result_to_memory& new_ins = dynamic_cast<const instruction_add_result_to_memory&>(*it);

            messages_init_add_id = new_ins.added_messages_init();
            messages_childs_add_id = new_ins.added_messages_child();
        }

        for (const local_message_id& i: env.result_message_ids())
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data& d = env.created_messages_init()[i.id];
                    for (message* p: d.ii)
                    {
                        p->wait_requests();
                        delete p;
                    }
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data& d = env.added_messages_init()[i.id];
                    d.mes->wait_requests();
                    memory.add_message_init_with_id(d.mes, messages_init_add_id[i.id], d.type, d.ii);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    message_child_data& d = env.created_messages_child()[i.id];
                    for (message* p: d.pi)
                    {
                        p->wait_requests();
                        delete p;
                    }
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    message_child_add_data& d = env.added_messages_child()[i.id];
                    d.mes->wait_requests();
                    memory.add_message_child_with_id(d.mes, messages_childs_add_id[i.id], d.type, MESSAGE_ID_UNDEFINED, d.pi);
                    break;
                }
                default:
                    comm.abort(765);
            }
        }
    }

    int parallelizer::get_current_proc()
    { return comm.rank(); }

    int parallelizer::get_proc_count()
    { return comm.size(); }

    void parallelizer::clear()
    {
        while (ready_tasks.size())
            ready_tasks.pop();

        memory.clear();
    }

}
