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

    void parallelizer::execution(task* root)
    {
        task_graph tg;
        tg.add_task(root);
        execution(tg);
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

        std::vector<std::set<task_id>> contained_tasks(comm.size());
        for (process i = 0; i < comm.size(); ++i)
            for (task_id j = 0; j < memory.task_count(); ++j)
                contained_tasks[i].insert(j);

        std::vector<instruction> ins(comm.size());
        std::vector<std::vector<task_id>> assigned(comm.size());
        size_t all_assigned = 0;

        while (ready_tasks.size())
        {
            size_t sub = ready_tasks.size() / comm.size();
            size_t per = ready_tasks.size() % comm.size();

            for (process i = 1; i < comm.size(); ++i)
            {
                size_t px = sub + ((static_cast<size_t>(comm.size()) - i <= per) ? 1: 0);
                for (size_t j = 0; j < px; ++j)
                {
                    send_task_data(ready_tasks.front(), i, ins[i], versions, contained);
                    assigned[i].push_back(ready_tasks.front());
                    ready_tasks.pop();
                    ++all_assigned;
                }
            }
            for (size_t j = 0; j < sub; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
                ++all_assigned;
            }

            for (process i = 1; i < comm.size(); ++i)
                for (task_id j: assigned[i])
                    assign_task(j, i, ins[i], contained_tasks);

            for (process i = 1; i < comm.size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            while (all_assigned > 0)
            {
                if (assigned[0].size() > 0)
                {
                    task_id i = assigned[0].back();
                    std::vector<local_message_id> data, c_data;
                    for (message_id j: memory.get_task_data(i))
                        data.push_back({j, MESSAGE_SOURCE::TASK_ARG});
                    for (message_id j: memory.get_task_const_data(i))
                        c_data.push_back({j, MESSAGE_SOURCE::TASK_ARG_C});
                    task_data td = {memory.get_task_type(i), data, c_data};
                    task_environment te(td, i);

                    for (message_id j: memory.get_task_data(i))
                        memory.get_message(j)->wait_requests();

                    for (message_id j: memory.get_task_const_data(i))
                        memory.get_message(j)->wait_requests();

                    memory.get_task(i)->perform(te);
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

    void parallelizer::send_task_data(task_id tid, process proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
    {
        for (message_id i: memory.get_task_data(tid))
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

        for (message_id i: memory.get_task_const_data(tid))
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
    }

    void parallelizer::assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<task_id>>& com)
    {
        if (com[proc].find(tid) == com[proc].end())
        {
            ins.add_task_creation(tid, memory.get_task_type(tid), memory.get_task_data(tid), memory.get_task_const_data(tid));
            com[proc].insert(tid);
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
                instr_comm.send(memory.get_message_info(j.id()), proc);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                instr_comm.send(memory.get_message_info(j.id()), proc);
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

    void parallelizer::end_main_task(task_id tid, task_environment& env, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<task_id>>& con_t)
    {
        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_add_id;
        std::vector<task_id> tasks_child_id;
        std::vector<task_id> tasks_child_add_id;

        for (message_id i: memory.get_task_data(tid))
        {
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
            memory.update_version(i, memory.get_message_version(i) + 1);
        }

        for (message_id i: memory.get_task_data(tid))
        {
            memory.get_message(i)->wait_requests();
            memory.include_child_to_parent_recursive(i);
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
                        case MESSAGE_SOURCE::REFERENCE:
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
                        case MESSAGE_SOURCE::REFERENCE:
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
            std::vector<local_message_id> local_data, local_c_data;
            switch (i.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data& t = env.added_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data& t = env.added_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
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
                    case MESSAGE_SOURCE::REFERENCE:
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
                    case MESSAGE_SOURCE::REFERENCE:
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
                case TASK_SOURCE::SIMPLE:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_id.push_back(id);
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data& t = env.added_tasks()[i.id];
                    task_id id = memory.add_task(t.t, t.type, data_id, const_data_id);
                    tasks_add_id.push_back(id);
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_child_id.push_back(id);
                    memory.set_task_parent(id, tid);
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data& t = env.added_child_tasks()[i.id];
                    task_id id = memory.add_task(t.t, t.type, data_id, const_data_id);
                    tasks_child_add_id.push_back(id);
                    memory.set_task_parent(id, tid);
                    con_t[main_proc].insert(id);
                    break;
                }
            }
        }
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + tid_childs);

        for (const task_dependence& i: env.created_dependences())
        {
            task_id parent;
            task_id child;
            switch (i.parent.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    parent = tasks_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    parent = tasks_add_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    parent = tasks_child_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    parent = tasks_child_add_id[i.parent.id];
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
                case TASK_SOURCE::SIMPLE:
                {
                    child = tasks_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    child = tasks_add_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    child = tasks_child_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    child = tasks_child_add_id[i.child.id];
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
                ready_tasks.push(i);
        }

        for (task_id i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: tasks_add_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: tasks_child_add_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        update_ready_tasks(tid);
    }

    void parallelizer::wait_task(process proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<task_id>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv(&res_ins, proc);
        instruction::const_iterator it = res_ins.begin();
        const instruction_block& ins = *it;
        if (ins.command() != INSTRUCTION::TASK_RES)
            comm.abort(111);
        const instruction_task_result& result = dynamic_cast<const instruction_task_result&>(ins);
        
        task_id tid = result.id();
        task_data td;
        task_environment env(std::move(td), tid);
        res_ins.clear();

        comm.recv(&env, proc);
        env.wait_requests();

        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_add_id;
        std::vector<task_id> tasks_child_id;
        std::vector<task_id> tasks_child_add_id;

        for (message_id i: memory.get_task_data(tid))
        {
            comm.recv(memory.get_message(i), proc);
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
            ver[proc].insert(i);
            memory.update_version(i, memory.get_message_version(i) + 1);
        }

        for (message_id i: memory.get_task_data(tid))
        {
            memory.get_message(i)->wait_requests();
            memory.include_child_to_parent_recursive(i);
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
                        case MESSAGE_SOURCE::REFERENCE:
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
                        case MESSAGE_SOURCE::REFERENCE:
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
            std::vector<local_message_id> local_data, local_c_data;
            switch (i.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data& t = env.added_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data& t = env.added_child_tasks()[i.id];
                    local_data = t.data;
                    local_c_data = t.c_data;
                    ++tid_childs;
                    break;
                }
                default:
                    comm.abort(765);
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
                case TASK_SOURCE::SIMPLE:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_id.push_back(id);
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data& t = env.added_tasks()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_add_id.push_back(id);
                    con_t[main_proc].insert(id);
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_child_id.push_back(id);
                    memory.set_task_parent(id, tid);
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data& t = env.added_child_tasks()[i.id];
                    task_id id = memory.create_task(t.type, data_id, const_data_id);
                    tasks_child_add_id.push_back(id);
                    memory.set_task_parent(id, tid);
                    con_t[main_proc].insert(id);
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
                case TASK_SOURCE::SIMPLE:
                {
                    parent = tasks_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    parent = tasks_add_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    parent = tasks_child_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    parent = tasks_child_add_id[i.parent.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    parent = i.parent.id;
                    break;
                }
                default:
                    comm.abort(765);
            }
            switch (i.child.src)
            {
                case TASK_SOURCE::SIMPLE:
                {
                    child = tasks_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_A:
                {
                    child = tasks_add_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_C:
                {
                    child = tasks_child_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    child = tasks_child_add_id[i.child.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    child = i.child.id;
                    break;
                }
                default:
                    comm.abort(765);
            }

            memory.add_dependence(parent, child);
        }

        if (env.added_child_tasks().size() + env.added_messages_child().size() + env.added_messages_init().size() + env.added_tasks().size() > 0)
        {
            res_ins.add_add_result_to_memory(messages_init_add_id, messages_childs_add_id, tasks_add_id, tasks_child_add_id);
            instr_comm.send(&res_ins, proc);
        }

        for (task_id i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: tasks_add_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: tasks_child_add_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        update_ready_tasks(tid);
    }

    void parallelizer::update_ready_tasks(task_id tid)
    {
        task_id c_t = tid;
        while (1)
        {
            if (memory.get_task_created_childs(c_t) == 0)
                for (task_id i: memory.get_task_childs(c_t))
                {
                    memory.set_task_parents_count(i, memory.get_task_parents_count(i) - 1);
                    if (memory.get_task_parents_count(i) == 0)
                        ready_tasks.push(i);
                }
            else
                break;
            if (!memory.task_has_parent(c_t))
                break;
            else
            {
                c_t = memory.get_task_parent(c_t);
                memory.set_task_created_childs(c_t, memory.get_task_created_childs(c_t) - 1);
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
                    sendable* iib = message_init_factory::get_info(j.type());
                    instr_comm.recv(iib, main_proc);
                    memory.create_message_init_with_id(j.id(), j.type(), iib);
                    break;
                }
                case INSTRUCTION::MES_P_CREATE:
                {
                    const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                    sendable* pib = message_child_factory::get_info(j.type());
                    instr_comm.recv(pib, main_proc);
                    if (memory.message_contained(j.source()))
                        memory.get_message(j.source())->wait_requests();
                    memory.create_message_child_with_id(j.id(), j.type(), j.source(), pib);
                    break;
                }
                case INSTRUCTION::TASK_CREATE:
                {
                    const instruction_task_create& j = dynamic_cast<const instruction_task_create&>(i);
                    memory.create_task_with_id(j.id(), j.type(), j.data(), j.const_data());
                    break;
                }
                case INSTRUCTION::TASK_EXE:
                {
                    const instruction_task_execute& j = dynamic_cast<const instruction_task_execute&>(i);
                    execute_task(j.id());
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

    void parallelizer::execute_task(task_id id)
    {
        std::vector<local_message_id> data, c_data;
        for (message_id i: memory.get_task_data(id))
            data.push_back({i, MESSAGE_SOURCE::TASK_ARG});
        for (message_id i: memory.get_task_const_data(id))
            c_data.push_back({i, MESSAGE_SOURCE::TASK_ARG_C});

        task_data td = {memory.get_task_type(id), data, c_data};
        task_environment env(std::move(td), id);

        for (message_id i: memory.get_task_data(id))
            memory.get_message(i)->wait_requests();

        for (message_id i : memory.get_task_const_data(id))
            memory.get_message(i)->wait_requests();

        memory.get_task(id)->perform(env);

        instruction res;
        res.add_task_result(id);

        instr_comm.send(&res, main_proc);

        comm.send(&env, main_proc);

        for (message_id i: memory.get_task_data(id))
            comm.send(memory.get_message(i), main_proc);

        for (const local_message_id& i: env.result_message_ids())
        {
            if (i.src == MESSAGE_SOURCE::INIT_A)
                comm.send(env.added_messages_init()[i.id].mes, main_proc);
            else if (i.src == MESSAGE_SOURCE::CHILD_A)
                comm.send(env.added_messages_child()[i.id].mes, main_proc);
        }
        env.wait_requests();

        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_add_id;

        if (env.added_child_tasks().size() + env.added_messages_child().size() + env.added_messages_init().size() + env.added_tasks().size() > 0)
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
                    delete d.ii;
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
                    delete d.pi;
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

        for (const local_task_id& i: env.result_task_ids())
        {
            switch (i.src)
            {
                case TASK_SOURCE::SIMPLE_A:
                {
                    task_add_data& t = env.added_tasks()[i.id];
                    delete t.t;
                    break;
                }
                case TASK_SOURCE::SIMPLE_AC:
                {
                    task_add_data& t = env.added_child_tasks()[i.id];
                    delete t.t;
                    break;
                }

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
