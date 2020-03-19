#include "parallelizer.h"

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
        for (size_t i = 0; i < comm.size(); ++i)
            for (size_t j = 0; j < memory.message_count(); ++j)
                contained[i].insert(j);

        std::vector<std::set<int>> contained_tasks(comm.size());
        for (int i = 0; i < comm.size(); ++i)
            for (size_t j = 0; j < memory.task_count(); ++j)
                contained_tasks[i].insert(j);

        std::vector<instruction> ins(comm.size());
        std::vector<std::vector<task_id>> assigned(comm.size());

        while (ready_tasks.size())
        {
            size_t sub = ready_tasks.size() / comm.size();
            size_t per = ready_tasks.size() % comm.size();

            for (size_t i = 1; i < comm.size(); ++i)
            {
                size_t px = sub + ((comm.size() - i <= per) ? 1: 0);
                for (size_t j = 0; j < px; ++j)
                {
                    send_task_data(ready_tasks.front(), static_cast<int>(i), ins[i], versions, contained);
                    assigned[i].push_back(ready_tasks.front());
                    ready_tasks.pop();
                }
            }
            for (size_t j = 0; j < sub; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
            }

            for (int i = 1; i < comm.size(); ++i)
                for (int j: assigned[i])
                    assign_task(j, i, ins[i], contained_tasks);

            for (int i = 1; i < comm.size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            for (task_id i: assigned[0])
            {
                std::vector<local_message_id> data, c_data;
                for (message_id j: memory.get_task_data(static_cast<size_t>(i)))
                    data.push_back({static_cast<int>(j), MESSAGE_SOURCE::TASK_ARG});
                for (message_id j: memory.get_task_const_data(i))
                    c_data.push_back({static_cast<int>(j), MESSAGE_SOURCE::TASK_ARG_C});
                task_data td = {memory.get_task_type(i), data, c_data};
                task_environment te(td);

                for (message_id j: memory.get_task_data(i))
                    memory.get_message(j)->wait_requests();

                for (message_id j: memory.get_task_const_data(i))
                    memory.get_message(j)->wait_requests();

                memory.get_task(i)->perform(te);
                end_main_task(i, te, versions, contained, contained_tasks);
            }
            assigned[0].clear();

            for (int i = 1; i < comm.size(); ++i)
            {
                for (int j = 0; j < assigned[i].size(); ++j)
                    wait_task(i, versions, contained, contained_tasks);
                assigned[i].clear();
            }
        }

        instruction end;
        end.add_end();
        for (int i = 1; i < instr_comm.size(); ++i)
            instr_comm.send(&end, i);
    }

    void parallelizer::send_task_data(task_id tid, int proc, instruction& ins, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
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

    void parallelizer::assign_task(task_id tid, int proc, instruction& ins, std::vector<std::set<int>>& com)
    {
        if (com[proc].find(tid) == com[proc].end())
        {
            ins.add_task_creation(tid, memory.get_task_type(tid), memory.get_task_data(tid), memory.get_task_const_data(tid));
            com[proc].insert(tid);
        }
        ins.add_task_execution(tid);
    }

    void parallelizer::send_instruction(int proc, instruction& ins)
    {
        instr_comm.send(&ins, proc);
        size_t j = 0;
        while (j < ins.size())
        {
            size_t g = j + 1;
            j += 2;
            switch (static_cast<INSTRUCTION>(ins[g - 1]))
            {
            case INSTRUCTION::MES_RECV:

                for (int i = 0; i < ins[g]; ++i)
                    comm.send(memory.get_message(ins[j++]), proc);
                break;

            case INSTRUCTION::MES_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(memory.get_message_init_info(ins[j]), proc);
                    j += 2;
                }
                break;

            case INSTRUCTION::MES_P_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(memory.get_message_part_info(ins[j]), proc);
                    j += 3;
                }
                break;

            case INSTRUCTION::TASK_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    j += 2;
                    j += ins[j];
                    ++j;
                    j += ins[j];
                    ++j;
                }
                break;

            case INSTRUCTION::TASK_EXE:

                for (int i = 0; i < ins[g]; ++i)
                    ++j;
                break;

            default:
                comm.abort(555);
            }
        }
    }

    void parallelizer::end_main_task(task_id tid, task_environment& te, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<int>>& con_t)
    {
        size_t new_mes = memory.message_count();
        std::vector<task_data>& td = te.get_c_tasks();
        std::vector<message_data>& md = te.get_c_messages();
        std::vector<message_part_data>& mpd = te.get_c_parts();

        std::vector<message_id> created_message_id;
        for (int i = 0; i < md.size(); ++i)
        {
            message_id id = memory.create_message(md[i].type, md[i].iib);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
            created_message_id.push_back(id);
        }

        for (int i = 0; i < mpd.size(); ++i)
        {
            message_id s_id;
            if ((mpd[i].sourse.ms != MESSAGE_SOURCE::TASK_ARG) && (mpd[i].sourse.ms != MESSAGE_SOURCE::TASK_ARG_C))
                s_id = created_message_id[mpd[i].sourse.id];
            else
                s_id = mpd[i].sourse.id;

            for (int k = 1; k < comm.size(); ++k)
                ver[k].erase(s_id);
            message_id id = memory.create_message(mpd[i].type, s_id, mpd[i].pib, mpd[i].iib);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
            created_message_id.push_back(id);
        }

        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + td.size());
        for (int i = 0; i < td.size(); ++i)
        {
            std::vector<message_id> data_id(td[i].data.size());
            for (int k = 0; k < td[i].data.size(); ++k)
            {
                if ((td[i].data[k].ms != MESSAGE_SOURCE::TASK_ARG) && (td[i].data[k].ms != MESSAGE_SOURCE::TASK_ARG_C))
                    data_id[k] = created_message_id[td[i].data[k].id];
                else
                    data_id[k] = td[i].data[k].id;
            }

            std::vector<message_id> const_data_id(td[i].c_data.size());
            for (int k = 0; k < td[i].c_data.size(); ++k)
            {
                size_t id = td[i].c_data[k].id;
                if ((td[i].c_data[k].ms != MESSAGE_SOURCE::TASK_ARG) && (td[i].c_data[k].ms != MESSAGE_SOURCE::TASK_ARG_C))
                    const_data_id[k] = created_message_id[td[i].data[k].id];
                else
                    const_data_id[k] = td[i].c_data[k].id;
            }

            task_id id = memory.create_task(td[i].type, data_id, const_data_id);
            memory.set_task_parent(id, tid);
            con_t[main_proc].insert(id);
            ready_tasks.push(id);
        }

        for (message_id i: memory.get_task_data(tid))
        {
            for (int k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
        }
        memory.update_message_versions(tid);

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

    void parallelizer::wait_task(int proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<int>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv(&res_ins, proc);

        int j = 0;
        j += 2;
        task_id tid = res_ins[j++];
        std::vector<message_id> created_id;

        for (message_id i: memory.get_task_data(tid))
        {
            comm.recv(memory.get_message(i), proc);
            for (int k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[main_proc].insert(i);
            ver[proc].insert(i);
            memory.update_version(i, memory.get_message_version(i) + 1);
        }

        for (message_id i: memory.get_task_data(tid))
            memory.get_message(i)->wait_requests();

        int sz = res_ins[j++];
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            message::init_info_base* iib = message_factory::get_info(type);
            instr_comm.recv(iib, proc);
            message_id id = memory.create_message(type, iib);
            created_id.push_back(id);
            con[main_proc].insert(static_cast<int>(id));
            ver[main_proc].insert(id);
        }

        sz = res_ins[j++];
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            size_t src = res_ins[j++];
            MESSAGE_SOURCE s_src = static_cast<MESSAGE_SOURCE>(res_ins[j++]);
            message::init_info_base* iib = message_factory::get_info(type);
            message::part_info_base* pib = message_factory::get_part_info(type);
            instr_comm.recv(iib, proc);
            instr_comm.recv(pib, proc);

            message_id s_id;
            if ((s_src != MESSAGE_SOURCE::TASK_ARG) && (s_src != MESSAGE_SOURCE::TASK_ARG_C))
                s_id += created_id[src];
            else
                s_id = src;

            memory.get_message(s_id)->wait_requests();

            for (int k = 1; k < comm.size(); ++k)
                ver[k].erase(static_cast<int>(s_id));

            message_id id = memory.create_message(type, s_id, pib, iib);
            created_id.push_back(id);
            con[main_proc].insert(static_cast<int>(id));
            ver[main_proc].insert(id);
        }

        sz = res_ins[j++];
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + sz);
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            int dsz = res_ins[j++];

            std::vector<message_id> data_id(dsz);
            for (int k = 0; k < dsz; ++k)
            {
                message_id id;
                size_t src_id = res_ins[j++];
                MESSAGE_SOURCE src = static_cast<MESSAGE_SOURCE>(res_ins[j++]);
                if ((src != MESSAGE_SOURCE::TASK_ARG) && (src != MESSAGE_SOURCE::TASK_ARG_C))
                    id = created_id[src_id];
                else
                    id = src_id;
                data_id[k] = id;
            }

            dsz = res_ins[j++];
            std::vector<message_id> const_data_id(dsz);
            for (int k = 0; k < dsz; ++k)
            {
                message_id id;
                size_t src_id = res_ins[j++];
                MESSAGE_SOURCE src = static_cast<MESSAGE_SOURCE>(res_ins[j++]);
                if ((src != MESSAGE_SOURCE::TASK_ARG) && (src != MESSAGE_SOURCE::TASK_ARG_C))
                    id = created_id[src_id];
                else
                    id = src_id;
                const_data_id[k] = id;
            }

            task_id id = memory.create_task(type, data_id, const_data_id);
            memory.set_task_parent(id, tid);
            con_t[main_proc].insert(static_cast<int>(id));
            ready_tasks.push(id);
        }

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
            size_t j = 0;

            while (j < cur_inst.size())
            {
                size_t cur_i_pos = j;
                j += 2;
                switch (static_cast<INSTRUCTION>(cur_inst[cur_i_pos]))
                {
                case INSTRUCTION::MES_SEND:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.send(memory.get_message(cur_inst[j++]), main_proc);
                    break;

                case INSTRUCTION::MES_RECV:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.recv(memory.get_message(cur_inst[j++]), main_proc);
                    break;

                case INSTRUCTION::MES_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                    {
                        create_message(cur_inst[j], cur_inst[j + 1], main_proc);
                        j += 2;
                    }
                    break;

                case INSTRUCTION::MES_P_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                    {
                        create_part(cur_inst[j], cur_inst[j + 1], cur_inst[j + 2], main_proc);
                        j += 3;
                    }
                    break;

                case INSTRUCTION::TASK_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        j += create_task(&cur_inst[j]);
                    break;

                case INSTRUCTION::TASK_EXE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        execute_task(cur_inst[j++]);
                    break;

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

    void parallelizer::create_message(message_id id, int type, int proc)
    {
        message::init_info_base* iib = message_factory::get_info(type);
        instr_comm.recv(iib, proc);
        memory.create_message_with_id(id, type, iib);
    }

    void parallelizer::create_part(message_id id, int type, int source, int proc)
    {
        message::part_info_base* pib = message_factory::get_part_info(type);
        instr_comm.recv(pib, proc);
        memory.get_message(source)->wait_requests();
        memory.create_message_with_id(id, type, source, pib);
    }

    int parallelizer::create_task(int* inst)
    {
        int ret = 4;
        int sz = inst[2];
        ret += sz;
        int* p = inst + 3;

        std::vector<message_id> data_id;
        data_id.reserve(sz);

        for (int i = 0; i < sz; ++i)
            data_id.push_back(p[i]);

        p += static_cast<size_t>(sz) + 1;
        sz = *(p - 1);
        ret += sz;

        std::vector<message_id> const_data_id;
        const_data_id.reserve(sz);

        for (int i = 0; i < sz; ++i)
            const_data_id.push_back(p[i]);
        memory.create_task_with_id(inst[0], inst[1], data_id, const_data_id);
        return ret;
    }

    void parallelizer::execute_task(task_id id)
    {
        std::vector<local_message_id> data, c_data;
        for (message_id i: memory.get_task_data(id))
            data.push_back({static_cast<int>(i), MESSAGE_SOURCE::TASK_ARG});
        for (message_id i: memory.get_task_const_data(id))
            c_data.push_back({static_cast<int>(i), MESSAGE_SOURCE::TASK_ARG_C});

        task_data td = {memory.get_task_type(id), data, c_data};
        task_environment env(std::move(td));

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

        for (int i = 0; i < env.get_c_messages().size(); ++i)
            instr_comm.send(env.get_c_messages()[i].iib, main_proc);
        for (int i = 0; i < env.get_c_parts().size(); ++i)
        {
            instr_comm.send(env.get_c_parts()[i].iib, main_proc);
            instr_comm.send(env.get_c_parts()[i].pib, main_proc);
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
