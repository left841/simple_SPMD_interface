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
        ready_tasks = memory.get_ready_tasks();
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
        std::vector<std::set<int>> versions(comm.size());
        for (std::set<int>& i: versions)
            for (int j = 0; j < memory.message_count(); ++j)
                i.insert(j);

        std::vector<std::set<int>> contained(comm.size());
        for (int i = 0; i < comm.size(); ++i)
            for (int j = 0; j < memory.message_count(); ++j)
                contained[i].insert(j);

        std::vector<std::set<int>> contained_tasks(comm.size());
        for (int i = 0; i < comm.size(); ++i)
            for (int j = 0; j < memory.task_count(); ++j)
                contained_tasks[i].insert(j);

        std::vector<instruction> ins(comm.size());
        std::vector<std::vector<int>> assigned(comm.size());

        while (ready_tasks.size())
        {
            size_t sub = ready_tasks.size() / comm.size();
            size_t per = ready_tasks.size() % comm.size();

            for (size_t i = 1; i < comm.size(); ++i)
            {
                size_t px = sub + ((comm.size() - i <= per) ? 1: 0);
                for (int j = 0; j < px; ++j)
                {
                    send_task_data(ready_tasks.front(), static_cast<int>(i), ins[i], versions, contained);
                    assigned[i].push_back(ready_tasks.front());
                    ready_tasks.pop();
                }
            }
            for (int j = 0; j < sub; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
            }

            for (int i = 1; i < comm.size(); ++i)
                for (int j: assigned[i])
                {
                    assign_task(j, i, ins[i], contained_tasks);
                }

            for (int i = 1; i < comm.size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            for (int i: assigned[0])
            {
                task_environment::task_info ti;
                for (message_id j: memory.get_task_data(i))
                    ti.data.push_back({j, task_environment::message_source::TASK_ARG});
                for (message_id j: memory.get_task_const_data(i))
                    ti.c_data.push_back({j, task_environment::message_source::TASK_ARG_C});
                task_environment::task_data td = {memory.get_task_type(i), &ti};
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

    void parallelizer::send_task_data(int tid, int proc, instruction& ins, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con)
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

    void parallelizer::assign_task(int tid, int proc, instruction& ins, std::vector<std::set<int>>& com)
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
            switch (static_cast<instruction::cmd>(ins[g - 1]))
            {
            case instruction::cmd::MES_RECV:

                for (int i = 0; i < ins[g]; ++i)
                    comm.send(memory.get_message(j++), proc);
                break;

            case instruction::cmd::MES_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(memory.get_message_init_info(ins[j]), proc);
                    j += 2;
                }
                break;

            case instruction::cmd::MES_P_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(memory.get_message_part_info(ins[j]), proc);
                    j += 3;
                }
                break;

            case instruction::cmd::TASK_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    j += 2;
                    j += ins[j];
                    ++j;
                    j += ins[j];
                    ++j;
                }
                break;

            case instruction::cmd::TASK_EXE:

                for (int i = 0; i < ins[g]; ++i)
                    ++j;
                break;

            default:
                comm.abort(555);
            }
        }
    }

    void parallelizer::end_main_task(int tid, task_environment& te, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t)
    {
        size_t new_mes = memory.message_count();
        std::vector<task_environment::task_data>& td = te.get_c_tasks();
        std::vector<task_environment::message_data>& md = te.get_c_messages();
        std::vector<task_environment::message_part_data>& mpd = te.get_c_parts();

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
            if ((mpd[i].sourse.ms != task_environment::message_source::TASK_ARG) && (mpd[i].sourse.ms != task_environment::message_source::TASK_ARG_C))
                s_id = created_message_id[mpd[i].sourse.id];
            else
                s_id = mpd[i].sourse.id;

            for (int k = 1; k < comm.size(); ++k)
                ver[k].erase(static_cast<int>(s_id));
            message_id id = memory.create_message(mpd[i].type, s_id, mpd[i].pib, mpd[i].iib);
            con[main_proc].insert(id);
            ver[main_proc].insert(id);
            created_message_id.push_back(id);
        }

        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + td.size());
        for (int i = 0; i < td.size(); ++i)
        {
            std::vector<message_id> data_id(td[i].ti->data.size());
            for (int k = 0; k < td[i].ti->data.size(); ++k)
            {
                if ((td[i].ti->data[k].ms != task_environment::message_source::TASK_ARG) && (td[i].ti->data[k].ms != task_environment::message_source::TASK_ARG_C))
                    data_id[k] = created_message_id[td[i].ti->data[k].id];
                else
                    data_id[k] = td[i].ti->data[k].id;
            }

            std::vector<message_id> const_data_id(td[i].ti->c_data.size());
            for (int k = 0; k < td[i].ti->c_data.size(); ++k)
            {
                size_t id = td[i].ti->c_data[k].id;
                if ((td[i].ti->c_data[k].ms != task_environment::message_source::TASK_ARG) && (td[i].ti->c_data[k].ms != task_environment::message_source::TASK_ARG_C))
                    const_data_id[k] = created_message_id[td[i].ti->data[k].id];
                else
                    const_data_id[k] = td[i].ti->c_data[k].id;
            }

            task_id id = memory.create_task(td[i].type, data_id, const_data_id);
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
            if (memory.get_task_created_childs(tid) == 0)
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

    void parallelizer::wait_task(int proc, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv(&res_ins, proc);

        int j = 0;
        j += 2;
        int tid = res_ins[j++];
        size_t new_mes = data_v.size();

        std::vector<int>& d = task_v[tid].data_id;
        for (int i = 0; i < d.size(); ++i)
        {
            comm.recv(data_v[d[i]].d, proc);
            for (int k = 0; k < comm.size(); ++k)
                ver[k].erase(d[i]);
            ver[main_proc].insert(d[i]);
            ver[proc].insert(d[i]);
            data_v[d[i]].version++;
        }

        for (int i = 0; i < d.size(); ++i)
            data_v[d[i]].d->wait_requests();

        int sz = res_ins[j++];
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            message::init_info_base* iib = message_factory::get_info(type);
            instr_comm.recv(iib, proc);
            message* m = message_factory::get(type, iib);
            d_info di = {m, type, iib, nullptr, -1, 0};
            data_v.push_back(di);
            con[main_proc].insert(static_cast<int>(data_v.size() - 1));
            ver[main_proc].insert(static_cast<int>(data_v.size() - 1));
        }
        sz = res_ins[j++];
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            size_t s_id = res_ins[j++];
            task_environment::message_source s_src = static_cast<task_environment::message_source>(res_ins[j++]);
            message::init_info_base* iib = message_factory::get_info(type);
            message::part_info_base* pib = message_factory::get_part_info(type);
            instr_comm.recv(iib, proc);
            instr_comm.recv(pib, proc);
            if ((s_src != task_environment::message_source::TASK_ARG) && (s_src != task_environment::message_source::TASK_ARG_C))
                s_id += new_mes;
            data_v[s_id].d->wait_requests();
            for (int k = 1; k < comm.size(); ++k)
                ver[k].erase(static_cast<int>(s_id));
            message* m = message_factory::get_part(type, data_v[s_id].d, pib);
            d_info di = {m, type, iib, pib, static_cast<int>(s_id), data_v[s_id].version};
            data_v.push_back(di);
            con[main_proc].insert(static_cast<int>(data_v.size() - 1));
            ver[main_proc].insert(static_cast<int>(data_v.size() - 1));
        }
        sz = res_ins[j++];
        task_v[tid].c_childs += sz;
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            int dsz = res_ins[j++];
            std::vector<message*> data(dsz);
            std::vector<int> data_id(dsz);
            for (int k = 0; k < dsz; ++k)
            {
                size_t id = res_ins[j++];
                task_environment::message_source src = static_cast<task_environment::message_source>(res_ins[j++]);
                if ((src != task_environment::message_source::TASK_ARG) && (src != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                data_id[k] = static_cast<int>(id);
                data[k] = data_v[id].d;
            }

            dsz = res_ins[j++];
            std::vector<const message*> c_data(dsz);
            std::vector<int> const_data_id(dsz);
            for (int k = 0; k < dsz; ++k)
            {
                size_t id = res_ins[j++];
                task_environment::message_source src = static_cast<task_environment::message_source>(res_ins[j++]);
                if ((src != task_environment::message_source::TASK_ARG) && (src != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                const_data_id[k] = static_cast<int>(id);
                c_data[k] = data_v[id].d;
            }

            task* t = task_factory::get(type, data, c_data);
            t_info ti = {t, type, tid, 0, 0, std::vector<int>(), data_id, const_data_id};
            task_v.push_back(ti);
            con_t[main_proc].insert(static_cast<int>(task_v.size() - 1));
            ready_tasks.push(static_cast<int>(task_v.size() - 1));
        }

        int c_t = tid;
        while (1)
        {
            if (task_v[c_t].c_childs == 0)
                for (int i : task_v[c_t].childs)
                {
                    --task_v[i].parents;
                    if (task_v[i].parents == 0)
                        ready_tasks.push(i);
                }
            else
                break;
            if (task_v[c_t].parent == -1)
                break;
            else
            {
                c_t = task_v[c_t].parent;
                --task_v[c_t].c_childs;
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
                switch (static_cast<instruction::cmd>(cur_inst[cur_i_pos]))
                {
                case instruction::cmd::MES_SEND:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.send(memory.get_message(cur_inst[j++]), main_proc);
                    break;

                case instruction::cmd::MES_RECV:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.recv(memory.get_message(cur_inst[j++]), main_proc);
                    break;

                case instruction::cmd::MES_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                    {
                        create_message(cur_inst[j], cur_inst[j + 1], main_proc);
                        j += 2;
                    }
                    break;

                case instruction::cmd::MES_P_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                    {
                        create_part(cur_inst[j], cur_inst[j + 1], cur_inst[j + 2], main_proc);
                        j += 3;
                    }
                    break;

                case instruction::cmd::TASK_CREATE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        j += create_task(&cur_inst[j]);
                    break;

                case instruction::cmd::TASK_EXE:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        execute_task(cur_inst[j++]);
                    break;

                case instruction::cmd::END:

                    goto end;

                default:

                    instr_comm.abort(234);
                }
            }
            cur_inst.clear();
        }
        end:;
    }

    void parallelizer::create_message(int id, int type, int proc)
    {
        message::init_info_base* iib = message_factory::get_info(type);
        instr_comm.recv(iib, proc);
        memory.create_message_with_id(id, type, iib);
    }

    void parallelizer::create_part(int id, int type, int source, int proc)
    {
        message::part_info_base* pib = message_factory::get_part_info(type);
        instr_comm.recv(pib, proc);
        message* src = data_v[source].d;
        src->wait_requests();
        if (data_v.size() <= id)
            data_v.resize(static_cast<size_t>(id) + 1);
        data_v[id] = {message_factory::get_part(type, src, pib), type, nullptr, pib, source, data_v[source].version};
    }

    int parallelizer::create_task(int* inst)
    {
        int ret = 4;
        int sz = inst[2];
        ret += sz;
        int* p = inst + 3;

        std::vector<message*> data;
        data.reserve(sz);
        std::vector<int> data_id;
        data_id.reserve(sz);

        for (int i = 0; i < sz; ++i)
        {
            data.push_back(data_v[p[i]].d);
            data_id.push_back(p[i]);
        }

        p += static_cast<size_t>(sz) + 1;
        sz = *(p - 1);
        ret += sz;

        std::vector<const message*> c_data;
        c_data.reserve(sz);
        std::vector<int> const_data_id;
        const_data_id.reserve(sz);

        for (int i = 0; i < sz; ++i)
        {
            c_data.push_back(data_v[p[i]].d);
            const_data_id.push_back(p[i]);
        }

        if (task_v.size() <= inst[0])
            task_v.resize(static_cast<size_t>(inst[0]) + 1);
        task_v[inst[0]] = {task_factory::get(inst[1], data, c_data), inst[1], -1, 0, 0, std::vector<int>(), data_id, const_data_id};

        return ret;
    }

    void parallelizer::execute_task(int task_id)
    {
        std::vector<int>& d = task_v[task_id].data_id;
        std::vector<int>& cd = task_v[task_id].const_data_id;

        task_environment::task_info ti;
        for (int i: d)
            ti.data.push_back({i, task_environment::message_source::TASK_ARG});
        for (int i: cd)
            ti.c_data.push_back({i, task_environment::message_source::TASK_ARG_C});

        task_environment::task_data td = {task_v[task_id].type, &ti};
        task_environment env(std::move(td));

        for (int i = 0; i < d.size(); ++i)
            data_v[d[i]].d->wait_requests();

        for (int i = 0; i < cd.size(); ++i)
            data_v[cd[i]].d->wait_requests();

        task_v[task_id].t->perform(env);

        instruction res;
        res.add_task_result(task_id, env);

        instr_comm.send(&res, main_proc);

        for (size_t i = 0; i < d.size(); ++i)
            comm.send(data_v[d[i]].d, main_proc);

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
