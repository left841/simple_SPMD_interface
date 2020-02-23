#include "parallelizer.h"

namespace auto_parallel
{

    parallelizer::instruction::instruction(): sendable()
    {
        previous = cmd::UNDEFINED;
        prev_pos = -1;
    }

    parallelizer::instruction::~instruction()
    { }

    void parallelizer::instruction::send(const sender& se)
    { se.send(v.data(), v.size(), MPI_INT); }

    void parallelizer::instruction::recv(const receiver& re)
    {
        v.resize(re.probe(MPI_INT));
        re.recv(v.data(), v.size(), MPI_INT);
    }

    int& parallelizer::instruction::operator[](size_t n)
    { return v[n]; }

    const int& parallelizer::instruction::operator[](size_t n) const
    { return v[n]; }

    size_t parallelizer::instruction::size()
    { return v.size(); }

    void parallelizer::instruction::clear()
    { 
        v.clear();
        previous = cmd::UNDEFINED;
    }

    void parallelizer::instruction::add_cmd(cmd id)
    {
        if (id == previous)
            ++v[prev_pos + 1];
        else
        {
            previous = id;
            prev_pos = v.size();
            v.push_back(static_cast<int>(id));
            v.push_back(1);
        }
    }

    void parallelizer::instruction::add_end()
    { add_cmd(cmd::END); }

    void parallelizer::instruction::add_message_sending(int id)
    {
        add_cmd(cmd::MES_SEND);
        v.push_back(id);
    }

    void parallelizer::instruction::add_message_receiving(int id)
    {
        add_cmd(cmd::MES_RECV);
        v.push_back(id);
    }

    void parallelizer::instruction::add_message_creation(int id, int type)
    {
        add_cmd(cmd::MES_CREATE);
        v.push_back(id);
        v.push_back(type);
    }

    void parallelizer::instruction::add_message_part_creation(int id, int type, int source)
    {
        add_cmd(cmd::MES_P_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(source);
    }

    void parallelizer::instruction::add_task_execution(int id)
    {
        add_cmd(cmd::TASK_EXE);
        v.push_back(id);
    }

    void parallelizer::instruction::add_task_creation(int id, int type, std::vector<int> data, std::vector<int> c_data)
    {
        add_cmd(cmd::TASK_CREATE);
        v.push_back(id);
        v.push_back(type);
        v.push_back(data.size());
        for (int i: data)
            v.push_back(i);
        v.push_back(c_data.size());
        for (int i: c_data)
            v.push_back(i);
    }

    void parallelizer::instruction::add_task_result(int id, task_environment& env)
    {
        add_cmd(cmd::TASK_RES);
        std::vector<task_environment::message_data>& md = env.get_c_messages();
        std::vector<task_environment::message_part_data>& mpd = env.get_c_parts();
        std::vector<task_environment::task_data>& td = env.get_c_tasks();

        v.push_back(id);
        v.push_back(md.size());
        for (int i = 0; i < md.size(); ++i)
            v.push_back(md[i].type);
        v.push_back(mpd.size());
        for (int i = 0; i < mpd.size(); ++i)
        {
            v.push_back(mpd[i].type);
            v.push_back(mpd[i].sourse.id);
            v.push_back(static_cast<int>(mpd[i].sourse.ms));
        }
        v.push_back(td.size());
        for (int i = 0; i < td.size(); ++i)
        {
            v.push_back(td[i].type);
            v.push_back(td[i].ti->data.size());
            for (task_environment::mes_id j: td[i].ti->data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
            v.push_back(td[i].ti->c_data.size());
            for (task_environment::mes_id j: td[i].ti->c_data)
            {
                v.push_back(j.id);
                v.push_back(static_cast<int>(j.ms));
            }
        }
    }

    const int parallelizer::main_proc = 0;

    parallelizer::parallelizer(int* argc, char*** argv): parallel_engine(argc, argv), comm(MPI_COMM_WORLD), instr_comm(comm)
    { }

    parallelizer::parallelizer(task_graph& _tg, int* argc, char*** argv): parallel_engine(argc, argv), comm(MPI_COMM_WORLD), instr_comm(comm)
    { init(_tg); }

    parallelizer::~parallelizer()
    { }

    void parallelizer::init(task_graph& _tg)
    {
        while (ready_tasks.size())
            ready_tasks.pop();

        task_v.clear();
        data_v.clear();
        task_v.resize(_tg.t_map.size());
        data_v.resize(_tg.d_map.size());

        std::map<task*, int> tmp;
        std::map<message*, int> dmp;
        std::map<int, task*> tmpr;
        std::map<int, message*> dmpr;
        unsigned i = 0;

        for (auto it = _tg.d_map.begin(); it != _tg.d_map.end(); ++it)
            dmpr[(*it).second.id] = (*it).first;

        for (auto it = dmpr.begin(); it != dmpr.end(); ++it, ++i)
        {
            dmp[(*it).second] = i;
            data_v[i].d = (*it).second;
            data_v[i].version = 0;
            data_v[i].type = -1;
            data_v[i].parent = -1;
            data_v[i].iib = nullptr;
            data_v[i].pib = nullptr;
        }
        dmpr.clear();

        for (auto it = _tg.t_map.begin(); it != _tg.t_map.end(); ++it)
            tmpr[(*it).second.id] = (*it).first;

        i = 0;
        for (auto it = tmpr.begin(); it != tmpr.end(); ++it, ++i)
        {
            tmp[(*it).second] = i;
            task_v[i].t = (*it).second;
            task_v[i].parent = -1;
            task_v[i].type = -1;
            task_v[i].c_childs = 0;
            task_v[i].parents = int((*_tg.t_map.find((*it).second)).second.parents.size());
            if (task_v[i].parents == 0)
                ready_tasks.push(i);
        }
        tmpr.clear();

        for (i = 0; i < task_v.size(); ++i)
        {
            const std::set<task*>& tp = (*_tg.t_map.find(task_v[i].t)).second.childs;
            task_v[i].childs.resize(tp.size());
            size_t j = 0;
            for (auto it = tp.begin(); it != tp.end(); ++it, ++j)
                task_v[i].childs[j] = tmp[*it];
            for (j = 0; j < task_v[i].t->data.size(); ++j)
                task_v[i].data_id.push_back(dmp[task_v[i].t->data[j]]);
            for (j = 0; j < task_v[i].t->c_data.size(); ++j)
            {
                message* t = const_cast<message*>(task_v[i].t->c_data[j]);
                task_v[i].const_data_id.push_back(dmp[t]);
            }
        }
        _tg.clear();

        top_versions.resize(data_v.size());
        top_versions.assign(data_v.size(), 0);
    }

    void parallelizer::execution()
    {
        if (comm.get_rank() == main_proc)
            master();
        else
            worker();

        task_v.clear();
        data_v.clear();
    }

    void parallelizer::master()
    {
        std::vector<std::set<int>> versions(comm.get_size());
        for (std::set<int>& i: versions)
            for (int j = 0; j < data_v.size(); ++j)
                i.insert(j);

        std::vector<std::set<int>> contained(comm.get_size());
        for (int i = 0; i < comm.get_size(); ++i)
            for (int j = 0; j < data_v.size(); ++j)
                contained[i].insert(j);

        std::vector<std::set<int>> contained_tasks(comm.get_size());
        for (int i = 0; i < comm.get_size(); ++i)
            for (int j = 0; j < task_v.size(); ++j)
                contained_tasks[i].insert(j);

        std::vector<instruction> ins(comm.get_size());
        std::vector<std::vector<int>> assigned(comm.get_size());

        while (ready_tasks.size())
        {
            int sub = ready_tasks.size() / comm.get_size();
            int per = ready_tasks.size() % comm.get_size();

            int rr = ready_tasks.size();

            for (int i = 1; i < comm.get_size(); ++i)
            {
                int px = sub + ((comm.get_size() - i <= per) ? 1: 0);
                for (int j = 0; j < px; ++j)
                {
                    send_task_data(ready_tasks.front(), i, ins[i], versions, contained);
                    assigned[i].push_back(ready_tasks.front());
                    ready_tasks.pop();
                }
            }
            for (int j = 0; j < sub; ++j)
            {
                assigned[0].push_back(ready_tasks.front());
                ready_tasks.pop();
            }

            for (int i = 1; i < comm.get_size(); ++i)
                for (int j: assigned[i])
                {
                    assign_task(j, i, ins[i], contained_tasks);
                }

            for (int i = 1; i < comm.get_size(); ++i)
            {
                send_instruction(i, ins[i]);
                ins[i].clear();
            }

            for (int i: assigned[0])
            {
                task_environment::task_info ti;
                for (int j: task_v[i].data_id)
                    ti.data.push_back({j, task_environment::message_source::TASK_ARG});
                for (int j: task_v[i].const_data_id)
                    ti.c_data.push_back({j, task_environment::message_source::TASK_ARG_C});
                task_environment::task_data td = {task_v[i].type, &ti};
                task_environment te(td);

                for (int j = 0; j < task_v[i].data_id.size(); ++j)
                    data_v[task_v[i].data_id[j]].d->wait_requests();

                for (int j = 0; j < task_v[i].const_data_id.size(); ++j)
                    data_v[task_v[i].const_data_id[j]].d->wait_requests();

                task_v[i].t->perform(te);
                end_main_task(i, te, versions, contained, contained_tasks);
            }
            assigned[0].clear();

            for (int i = 1; i < comm.get_size(); ++i)
            {
                for (int j = 0; j < assigned[i].size(); ++j)
                    wait_task(i, versions, contained, contained_tasks);
                assigned[i].clear();
            }
        }

        instruction end;
        end.add_end();
        for (int i = 1; i < instr_comm.get_size(); ++i)
            instr_comm.send(&end, i);
    }

    void parallelizer::send_task_data(int tid, int proc, instruction& ins, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con)
    {
        std::vector<int>& d = task_v[tid].data_id;
        std::vector<int>& cd = task_v[tid].const_data_id;

        for (int i: d)
        {
            if (con[proc].find(i) == con[proc].end())
            {
                if ((data_v[i].parent != -1) && (con[proc].find(data_v[i].parent) != con[proc].end()))
                {
                    ins.add_message_part_creation(i, data_v[i].type, data_v[i].parent);
                    if (ver[proc].find(data_v[i].parent) == ver[proc].end())
                        ins.add_message_receiving(i);
                }
                else
                {
                    ins.add_message_creation(i, data_v[i].type);
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

        for (int i: cd)
        {
            if (con[proc].find(i) == con[proc].end())
            {
                if ((data_v[i].parent != -1) && (con[proc].find(data_v[i].parent) != con[proc].end()))
                {
                    ins.add_message_part_creation(i, data_v[i].type, data_v[i].parent);
                    if (ver[proc].find(data_v[i].parent) == ver[proc].end())
                        ins.add_message_receiving(i);
                }
                else
                {
                    ins.add_message_creation(i, data_v[i].type);
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
            ins.add_task_creation(tid, task_v[tid].type, task_v[tid].data_id, task_v[tid].const_data_id);
            com[proc].insert(tid);
        }
        ins.add_task_execution(tid);
    }

    void parallelizer::send_instruction(int proc, instruction& ins)
    {
        instr_comm.send(&ins, proc);
        int j = 0;
        while (j < ins.size())
        {
            int g = j + 1;
            j += 2;
            switch (static_cast<instruction::cmd>(ins[g - 1]))
            {
            case instruction::cmd::MES_RECV:

                for (int i = 0; i < ins[g]; ++i)
                    comm.send(data_v[ins[j++]].d, proc);
                break;

            case instruction::cmd::MES_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(data_v[ins[j]].iib, proc);
                    j += 2;
                }
                break;

            case instruction::cmd::MES_P_CREATE:

                for (int i = 0; i < ins[g]; ++i)
                {
                    instr_comm.send(data_v[ins[j]].pib, proc);
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
        int new_mes = data_v.size();
        std::vector<task_environment::task_data>& td = te.get_c_tasks();
        std::vector<task_environment::message_data>& md = te.get_c_messages();
        std::vector<task_environment::message_part_data>& mpd = te.get_c_parts();


        for (int i = 0; i < md.size(); ++i)
        {
            message* m = message_factory::get(md[i].type, md[i].iib);
            d_info di = {m, md[i].type, md[i].iib, nullptr, -1, 0};
            data_v.push_back(di);
            con[main_proc].insert(data_v.size() - 1);
            ver[main_proc].insert(data_v.size() - 1);
        }

        for (int i = 0; i < mpd.size(); ++i)
        {
            int s_id = mpd[i].sourse.id;
            if ((mpd[i].sourse.ms != task_environment::message_source::TASK_ARG) && (mpd[i].sourse.ms != task_environment::message_source::TASK_ARG_C))
                s_id += new_mes;
            for (int k = 1; k < comm.get_size(); ++k)
                ver[k].erase(s_id);
            message* m = message_factory::get_part(mpd[i].type, data_v[s_id].d, mpd[i].pib);
            d_info di = {m, mpd[i].type, mpd[i].iib, mpd[i].pib, s_id, data_v[s_id].version};
            data_v.push_back(di);
            con[main_proc].insert(data_v.size() - 1);
            ver[main_proc].insert(data_v.size() - 1);
        }

        task_v[tid].c_childs += td.size();
        for (int i = 0; i < td.size(); ++i)
        {
            std::vector<message*> data(td[i].ti->data.size());
            std::vector<int> data_id(td[i].ti->data.size());
            for (int k = 0; k < td[i].ti->data.size(); ++k)
            {
                int id = td[i].ti->data[k].id;
                if ((td[i].ti->data[k].ms != task_environment::message_source::TASK_ARG) && (td[i].ti->data[k].ms != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                data_id[k] = id;
                data[k] = data_v[id].d;
            }

            std::vector<const message*> c_data(td[i].ti->c_data.size());
            std::vector<int> const_data_id(td[i].ti->c_data.size());
            for (int k = 0; k < td[i].ti->c_data.size(); ++k)
            {
                int id = td[i].ti->c_data[k].id;
                if ((td[i].ti->c_data[k].ms != task_environment::message_source::TASK_ARG) && (td[i].ti->c_data[k].ms != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                const_data_id[k] = id;
                c_data[k] = data_v[id].d;
            }

            task* t = task_factory::get(td[i].type, data, c_data);
            t_info ti = {t, td[i].type, tid, 0, 0, std::vector<int>(), data_id, const_data_id};
            task_v.push_back(ti);
            con_t[main_proc].insert(task_v.size() - 1);
            ready_tasks.push(task_v.size() - 1);
        }

        std::vector<int>& d = task_v[tid].data_id;
        for (int i = 0; i < d.size(); ++i)
        {
            for (int k = 0; k < comm.get_size(); ++k)
                ver[k].erase(d[i]);
            ver[main_proc].insert(d[i]);
            data_v[d[i]].version++;
        }

        int c_t = tid;
        while (1)
        {
            if (task_v[c_t].c_childs == 0)
                for (int i: task_v[c_t].childs)
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

    void parallelizer::wait_task(int proc, std::vector<std::set<int>>& ver, std::vector<std::set<int>>& con, std::vector<std::set<int>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv(&res_ins, proc);

        int j = 0;
        j += 2;
        int tid = res_ins[j++];
        int new_mes = data_v.size();

        std::vector<int>& d = task_v[tid].data_id;
        for (int i = 0; i < d.size(); ++i)
        {
            comm.recv(data_v[d[i]].d, proc);
            for (int k = 0; k < comm.get_size(); ++k)
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
            con[main_proc].insert(data_v.size() - 1);
            ver[main_proc].insert(data_v.size() - 1);
        }
        sz = res_ins[j++];
        for (int i = 0; i < sz; ++i)
        {
            int type = res_ins[j++];
            int s_id = res_ins[j++];
            task_environment::message_source s_src = static_cast<task_environment::message_source>(res_ins[j++]);
            message::init_info_base* iib = message_factory::get_info(type);
            message::part_info_base* pib = message_factory::get_part_info(type);
            instr_comm.recv(iib, proc);
            instr_comm.recv(pib, proc);
            if ((s_src != task_environment::message_source::TASK_ARG) && (s_src != task_environment::message_source::TASK_ARG_C))
                s_id += new_mes;
            data_v[s_id].d->wait_requests();
            for (int k = 1; k < comm.get_size(); ++k)
                ver[k].erase(s_id);
            message* m = message_factory::get_part(type, data_v[s_id].d, pib);
            d_info di = {m, type, iib, pib, s_id, data_v[s_id].version};
            data_v.push_back(di);
            con[main_proc].insert(data_v.size() - 1);
            ver[main_proc].insert(data_v.size() - 1);
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
                int id = res_ins[j++];
                task_environment::message_source src = static_cast<task_environment::message_source>(res_ins[j++]);
                if ((src != task_environment::message_source::TASK_ARG) && (src != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                data_id[k] = id;
                data[k] = data_v[id].d;
            }

            dsz = res_ins[j++];
            std::vector<const message*> c_data(dsz);
            std::vector<int> const_data_id(dsz);
            for (int k = 0; k < dsz; ++k)
            {
                int id = res_ins[j++];
                task_environment::message_source src = static_cast<task_environment::message_source>(res_ins[j++]);
                if ((src != task_environment::message_source::TASK_ARG) && (src != task_environment::message_source::TASK_ARG_C))
                    id += new_mes;
                const_data_id[k] = id;
                c_data[k] = data_v[id].d;
            }

            task* t = task_factory::get(type, data, c_data);
            t_info ti = {t, type, tid, 0, 0, std::vector<int>(), data_id, const_data_id};
            task_v.push_back(ti);
            con_t[main_proc].insert(task_v.size() - 1);
            ready_tasks.push(task_v.size() - 1);
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
            int j = 0;

            while (j < cur_inst.size())
            {
                int cur_i_pos = j;
                j += 2;
                switch (static_cast<instruction::cmd>(cur_inst[cur_i_pos]))
                {
                case instruction::cmd::MES_SEND:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.send(data_v[cur_inst[j++]].d, main_proc);
                    break;

                case instruction::cmd::MES_RECV:

                    for (int i = 0; i < cur_inst[cur_i_pos + 1]; ++i)
                        comm.recv(data_v[cur_inst[j++]].d, main_proc);
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
        if (data_v.size() <= id)
            data_v.resize(id + 1);
        data_v[id] = {message_factory::get(type, iib), type, iib, nullptr, -1, 0};
    }

    void parallelizer::create_part(int id, int type, int source, int proc)
    {
        message::part_info_base* pib = message_factory::get_part_info(type);
        instr_comm.recv(pib, proc);
        message* src = data_v[source].d;
        src->wait_requests();
        if (data_v.size() <= id)
            data_v.resize(id + 1);
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

        p += sz + 1;
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
            task_v.resize(inst[0] + 1);
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
    { return comm.get_rank(); }

    int parallelizer::get_proc_count()
    { return comm.get_size(); }

}
