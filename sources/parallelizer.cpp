#include "parallelizer.h"
//#include <iostream>
//using std::cout;
//using std::endl;

namespace auto_parallel
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
                }
            }
            for (size_t j = 0; j < sub; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
            }

            for (process i = 1; i < comm.size(); ++i)
                for (task_id j: assigned[i])
                    assign_task(j, i, ins[i], contained_tasks);

            for (process i = 1; i < comm.size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            for (task_id i: assigned[0])
            {
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
            }
            assigned[0].clear();

            for (process i = 1; i < comm.size(); ++i)
            {
                for (size_t j = 0; j < assigned[i].size(); ++j)
                    wait_task(i, versions, contained, contained_tasks);
                assigned[i].clear();
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
                if (memory.message_has_parent(i) && (con[proc].find(memory.get_message_parent(i)) != con[proc].end()))
                {
                    ins.add_message_part_creation(i, memory.get_message_type(i), memory.get_message_parent(i));
                    if (ver[proc].find(memory.get_message_parent(i)) == ver[proc].end())
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
                if (memory.message_has_parent(i) && (con[proc].find(memory.get_message_parent(i)) != con[proc].end()))
                {
                    ins.add_message_part_creation(i, memory.get_message_type(i), memory.get_message_parent(i));
                    if (ver[proc].find(memory.get_message_parent(i)) == ver[proc].end())
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
                instr_comm.send(memory.get_message_init_info(j.id()), proc);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                instr_comm.send(memory.get_message_part_info(j.id()), proc);
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

    void parallelizer::end_main_task(task_id tid, task_environment& te, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<task_id>>& con_t)
    {
        std::vector<message_data>& md = te.created_messages();
        std::vector<message_part_data>& mpd = te.created_parts();
        std::vector<task_data>& td = te.created_child_tasks();
        std::vector<task_data>& cre_t = te.created_tasks();
        std::vector<task_dependence>& dep = te.created_dependences();

        std::vector<message_id> created_message_id;
        for (size_t i = 0; i < md.size(); ++i)
        {
            message_id id = memory.create_message(md[i].type, md[i].iib);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
            created_message_id.push_back(id);
        }

        std::vector<message_id> created_part_id;
        for (size_t i = 0; i < mpd.size(); ++i)
        {
            message_id s_id;
            if ((mpd[i].sourse.ms == MESSAGE_SOURCE::TASK_ARG) || (mpd[i].sourse.ms == MESSAGE_SOURCE::TASK_ARG_C))
                s_id = mpd[i].sourse.id;
            else if (mpd[i].sourse.ms == MESSAGE_SOURCE::CREATED)
                s_id = created_message_id[mpd[i].sourse.id];
            else
                s_id = created_part_id[mpd[i].sourse.id];

            for (process k = 1; k < comm.size(); ++k)
                ver[k].erase(s_id);
            message_id id = memory.create_message(mpd[i].type, s_id, mpd[i].pib, mpd[i].iib);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
            created_part_id.push_back(id);
        }

        std::vector<task_id> created_child_task_id;
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + td.size());
        for (task_data& i: td)
        {
            std::vector<message_id> data_id;
            data_id.reserve(i.data.size());
            for (local_message_id k: i.data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    data_id.push_back(created_message_id[k.id]);
                else
                    data_id.push_back(created_part_id[k.id]);
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(i.c_data.size());
            for (local_message_id k: i.c_data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    const_data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    const_data_id.push_back(created_message_id[k.id]);
                else
                    const_data_id.push_back(created_part_id[k.id]);
            }

            task_id id = memory.create_task(i.type, data_id, const_data_id);
            memory.set_task_parent(id, tid);
            con_t[main_proc].insert(id);
            created_child_task_id.push_back(id);
        }

        std::vector<task_id> created_task_id;
        for (task_data& i: cre_t)
        {
            std::vector<message_id> data_id;
            data_id.reserve(i.data.size());
            for (local_message_id k: i.data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    data_id.push_back(created_message_id[k.id]);
                else
                    data_id.push_back(created_part_id[k.id]);
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(i.c_data.size());
            for (local_message_id k: i.c_data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    const_data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    const_data_id.push_back(created_message_id[k.id]);
                else
                    const_data_id.push_back(created_part_id[k.id]);
            }

            task_id id = memory.create_task(i.type, data_id, const_data_id);
            con_t[main_proc].insert(id);
            created_task_id.push_back(id);
        }

        for (task_dependence& i: dep)
        {
            task_id parent;
            task_id child;
            if (i.parent.src == TASK_SOURCE::CHILD)
                parent = created_child_task_id[i.parent.id];
            else if (i.parent.src == TASK_SOURCE::CREATED)
                parent = created_task_id[i.parent.id];
            else
                parent = i.parent.id;

            if (i.child.src == TASK_SOURCE::CHILD)
                child = created_child_task_id[i.child.id];
            else if (i.child.src == TASK_SOURCE::CREATED)
                child = created_task_id[i.child.id];
            else
                child = i.child.id;
            memory.add_dependence(parent, child);
        }

        for (task_id i: created_child_task_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: created_task_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (message_id i: memory.get_task_data(tid))
        {
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
        }
        memory.update_message_versions(tid);

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

        std::vector<message_type>& mes_created = result.created_messages();
        std::vector<std::pair<message_type, local_message_id>>& part_created = result.created_parts();
        std::vector<task_data>& task_child_created = result.created_child_tasks();
        std::vector<task_data>& task_created = result.created_tasks();
        std::vector<task_dependence>& dependence_created = result.created_task_dependences();
        task_id tid = result.id();

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
            memory.get_message(i)->wait_requests();

        std::vector<message_id> created_message_id;
        for (message_type i: mes_created)
        {
            message::init_info_base* iib = message_factory::get_info(i);
            instr_comm.recv(iib, proc);
            message_id id = memory.create_message(i, iib);
            created_message_id.push_back(id);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
        }

        std::vector<message_id> created_part_id;
        for (std::pair<message_type, local_message_id> i: part_created)
        {
            message::init_info_base* iib = message_factory::get_info(i.first);
            message::part_info_base* pib = message_factory::get_part_info(i.first);
            instr_comm.recv(iib, proc);
            instr_comm.recv(pib, proc);

            message_id s_id;
            if ((i.second.ms == MESSAGE_SOURCE::TASK_ARG) || (i.second.ms == MESSAGE_SOURCE::TASK_ARG_C))
                s_id = i.second.id;
            else if (i.second.ms == MESSAGE_SOURCE::CREATED)
                s_id = created_message_id[i.second.id];
            else
                s_id = created_part_id[i.second.id];

            memory.get_message(s_id)->wait_requests();

            for (process k = 1; k < comm.size(); ++k)
                ver[k].erase(s_id);

            message_id id = memory.create_message(i.first, s_id, pib, iib);
            created_part_id.push_back(id);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
        }

        std::vector<task_id> created_child_task_id;
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + task_child_created.size());
        for (task_data& i: task_child_created)
        {
            std::vector<message_id> data_id;
            data_id.reserve(i.data.size());
            for (local_message_id k: i.data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    data_id.push_back(created_message_id[k.id]);
                else
                    data_id.push_back(created_part_id[k.id]);
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(i.c_data.size());
            for (local_message_id k: i.c_data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    const_data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    const_data_id.push_back(created_message_id[k.id]);
                else
                    const_data_id.push_back(created_part_id[k.id]);
            }

            task_id id = memory.create_task(i.type, data_id, const_data_id);
            memory.set_task_parent(id, tid);
            con_t[main_proc].insert(id);
            created_child_task_id.push_back(id);
        }

        std::vector<task_id> created_task_id;
        for (task_data& i: task_created)
        {
            std::vector<message_id> data_id;
            data_id.reserve(i.data.size());
            for (local_message_id k: i.data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    data_id.push_back(created_message_id[k.id]);
                else
                    data_id.push_back(created_part_id[k.id]);
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(i.c_data.size());
            for (local_message_id k: i.c_data)
            {
                if ((k.ms == MESSAGE_SOURCE::TASK_ARG) || (k.ms == MESSAGE_SOURCE::TASK_ARG_C))
                    const_data_id.push_back(k.id);
                else if (k.ms == MESSAGE_SOURCE::CREATED)
                    const_data_id.push_back(created_message_id[k.id]);
                else
                    const_data_id.push_back(created_part_id[k.id]);
            }

            task_id id = memory.create_task(i.type, data_id, const_data_id);
            con_t[main_proc].insert(id);
            created_task_id.push_back(id);
        }

        for (task_dependence& i: dependence_created)
        {
            task_id parent;
            task_id child;
            if (i.parent.src == TASK_SOURCE::CHILD)
                parent = created_child_task_id[i.parent.id];
            else if (i.parent.src == TASK_SOURCE::CREATED)
                parent = created_task_id[i.parent.id];
            else
                parent = i.parent.id;

            if (i.child.src == TASK_SOURCE::CHILD)
                child = created_child_task_id[i.child.id];
            else if (i.child.src == TASK_SOURCE::CREATED)
                child = created_task_id[i.child.id];
            else
                child = i.child.id;
            memory.add_dependence(parent, child);
        }

        for (task_id i: created_child_task_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i);
        }

        for (task_id i: created_task_id)
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
                    message::init_info_base* iib = message_factory::get_info(j.type());
                    instr_comm.recv(iib, main_proc);
                    memory.create_message_with_id(j.id(), j.type(), iib);
                    break;
                }
                case INSTRUCTION::MES_P_CREATE:
                {
                    const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                    message::part_info_base* pib = message_factory::get_part_info(j.type());
                    instr_comm.recv(pib, main_proc);
                    memory.get_message(j.source())->wait_requests();
                    memory.create_message_with_id(j.id(), j.type(), j.source(), pib);
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
        res.add_task_result(id, env);

        instr_comm.send(&res, main_proc);

        for (message_id i: memory.get_task_data(id))
            comm.send(memory.get_message(i), main_proc);

        for (int i = 0; i < env.created_messages().size(); ++i)
            instr_comm.send(env.created_messages()[i].iib, main_proc);
        for (int i = 0; i < env.created_parts().size(); ++i)
        {
            instr_comm.send(env.created_parts()[i].iib, main_proc);
            instr_comm.send(env.created_parts()[i].pib, main_proc);
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
