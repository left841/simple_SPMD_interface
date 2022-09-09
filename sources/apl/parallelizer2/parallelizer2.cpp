#include "apl/parallelizer2/parallelizer2.h"
#include <iostream>

namespace apl
{

    const process parallelizer2::main_proc = 0;

    parallelizer2::parallelizer2(const intracomm& _comm): comm(_comm), instr_comm(comm)
    { }

    parallelizer2::parallelizer2(task_graph& _tg, const intracomm& _comm): comm(_comm), instr_comm(comm), memory(_tg, main_proc)
    { }

    parallelizer2::~parallelizer2()
    { }

    void parallelizer2::execution(task_graph& _tg)
    {
        execution(2, _tg);
    }

    void parallelizer2::execution(size_t tree_param, task_graph& _tg)
    {
        memory.init(_tg, main_proc);

        exe_info info;

        memory_manager_graph_adapter adapter(memory);
        graph_analizer graph(adapter);

        {
            std::vector<processes_group> groups(std::move(make_topology(tree_param)));

            info.parent = groups[comm.rank()].owner;
            if (info.parent != MPI_PROC_NULL)
                external_comm = std::move(groups[info.parent].comm);
            info.childs_groups.resize(1);
            info.groups_of_processes.resize(comm.size());
        
            for (size_t i = 0, j = 1; i < groups.size(); ++i)
            {
                if (groups[i].owner == comm.rank())
                {
                    info.groups_of_processes[groups[i].head] = info.childs_groups.size();
                    info.childs_groups.push_back(std::move(groups[i]));
                    info.childs_groups.back().internal_head = j++;
                }
            }

            info.groups_of_processes[comm.rank()] = 0;
            internal_comm = std::move(groups[comm.rank()].comm);
            info.childs_groups[0] = std::move(groups[comm.rank()]);
            info.childs_groups[0].internal_head = 0;
            info.childs_groups[0].size = 1;

            if (internal_comm.get_comm() != MPI_COMM_NULL)
                info.internal_comm_size = internal_comm.size();
            else
                info.internal_comm_size = 1;
        }

        info.group_info_v.resize(info.childs_groups.size());
        info.group_workload.resize(info.childs_groups.size());

        info.versions_mes.resize(info.childs_groups.size());
        info.versions_mes[0] = memory.get_messages_set();

        for (size_t i = 1; i < info.internal_comm_size; ++i)
            info.versions_mes[i] = info.versions_mes[0];

        info.contained_mes.resize(info.childs_groups.size());
        for (size_t i = 0; i < info.internal_comm_size; ++i)
            info.contained_mes[i] = info.versions_mes[0];

        std::vector<instruction> internal_instructions(info.childs_groups.size());
        instruction external_instruction;

        if (comm.rank() == main_proc)
        {
            info.ready_tasks = memory.get_ready_tasks(main_proc);
            info.group_info_v[0].active_tasks = info.ready_tasks.size();
            info.all_active_tasks += info.group_info_v[0].active_tasks;
            info.group_info_v[0].all_tasks = memory.task_count();
            info.all_tasks += memory.task_count();
        }
        else
        {
            auto m = memory.get_performs_set();
            for (auto& i: m)
                memory.delete_perform(i);
        }

        info.exe = true;
        bool splited = false;
        while (info.exe)
        {
            ptrdiff_t old_active_tasks_count = info.all_active_tasks;
            //if (/*(info.all_active_tasks == 0) &&*/ /*(graph.need_split(info.childs_groups, info.group_info_v, info.ready_tasks.size())) &&*/ (comm.rank() == 0) && !splited)
            if (graph.need_split(info.childs_groups, info.group_info_v, info.ready_tasks.size()))
            {
                //splited = true;
                std::vector<sub_graph> tasks_for_childs = split_graph(info);

                transfer_state(tasks_for_childs, info, internal_instructions);
                info.ready_tasks = memory.get_ready_tasks();
            }

            process ext_proc = MPI_PROC_NULL;
            if (info.ready_tasks.empty())
                instr_comm.wait_any_process();
            while ((ext_proc = instr_comm.test_any_process()) != MPI_PROC_NULL)
            {
                instr_comm.recv<message>(&external_instruction, ext_proc);
                if (!process_instruction(external_instruction, info, ext_proc))
                    info.exe = false;
                external_instruction.clear();
            }

            if (info.ready_tasks.size())
            {
                perform_id up_id = info.ready_tasks.front();
                info.ready_tasks.pop_front();
                --info.group_info_v[0].active_tasks;
                --info.all_active_tasks;
                --info.group_info_v[0].all_tasks;
                --info.all_tasks;
                execute_task(up_id, info);
            }

            if ((info.parent != MPI_PROC_NULL) && ((old_active_tasks_count != info.all_active_tasks) || (info.all_tasks == 0)))
            {
                external_instruction.add_send_exe_stats(info.all_active_tasks, info.all_tasks);
                instr_comm.send<message>(&external_instruction, info.parent);
                external_instruction.clear();
            }

            if (((info.parent == MPI_PROC_NULL) && (info.all_tasks == 0)) || (info.exe == false))
            {
                for (size_t i = 1; i < info.group_info_v.size(); ++i)
                {
                    internal_instructions[i].add_end();
                    instr_comm.send<message>(&internal_instructions[i], info.childs_groups[i].head);
                }
                info.exe = false;
            }
        }

        clear();
    }

    std::vector<processes_group> parallelizer2::make_topology(size_t tree_param)
    {
        std::vector<processes_group> groups(comm.size());
        std::vector<process> proc_v;
        std::queue<process> proc_q;
        process this_owner = MPI_PROC_NULL;

        proc_q.push(0);
        groups[0].owner = MPI_PROC_NULL;
        process cur_proc = 1;
        while (proc_q.size())
        {
            process next = proc_q.front();
            proc_q.pop();
            proc_v.push_back(next);
            for (size_t i = 0; (i < tree_param) && (cur_proc < comm.size()); ++i)
            {
                proc_v.push_back(cur_proc);
                if (cur_proc == comm.rank())
                    this_owner = next;
                groups[cur_proc].owner = next;
                proc_q.push(cur_proc);
                ++cur_proc;
            }

            comm_group cur_group(comm.group(), proc_v.data(), proc_v.size(), group_constructor_type::incl);
            groups[next].head = next;
            groups[next].size = 1;
            groups[next].internal_head = MPI_PROC_NULL;
            comm.barrier();
            groups[next].comm.create(comm, cur_group);
            proc_v.clear();
        }
        for (size_t j = groups.size() - 1; j > 0; --j)
        {
            groups[groups[j].owner].size += 1;
        }
        groups[comm.rank()].owner = this_owner;

        return groups;
    }

    std::vector<sub_graph> parallelizer2::split_graph(exe_info& info)
    {
        std::vector<sub_graph> sub_graphs(info.childs_groups.size());
        info.group_info_v[0].active_tasks = 0;
        info.group_info_v[0].all_tasks = 0;
        std::map<perform_id, size_t> parents_count;
        std::map<perform_id, size_t> perform_group;

        std::set<size_t, std::function<bool(size_t, size_t)>> procs([&](size_t a, size_t b)->bool
        {
            if (info.group_info_v[a].active_tasks != info.group_info_v[b].active_tasks)
                return info.group_info_v[a].active_tasks < info.group_info_v[b].active_tasks;
            return a < b;
        });

        for (size_t i = 0; i < info.childs_groups.size(); ++i)
            procs.insert(i);

        while (info.ready_tasks.size())
        {
            std::queue<perform_id> q;
            q.push(info.ready_tasks.front());
            info.ready_tasks.pop_front();
            while (q.size())
            {
                perform_id id = q.front();
                q.pop();

                size_t cur_g = *procs.begin();
                procs.erase(cur_g);
                ++info.group_info_v[cur_g].active_tasks;
                procs.insert(cur_g);

                while (1)
                {
                    sub_graphs[cur_g].tasks.push_back(id);
                    perform_group[id] = cur_g;
                    if (memory.get_perform_childs(id).size() < 1)
                        break;
                    perform_id old_id = id;
                    for (size_t i = 1; i < memory.get_perform_childs(id).size(); ++i)
                    {
                        perform_id tid = memory.get_perform_childs(id)[i];
                        parents_count[tid]++;
                        if (parents_count[tid] >= memory.get_perform_parents_count(tid))
                            q.push(memory.get_perform_childs(id)[i]);
                    }
                    id = memory.get_perform_childs(id)[0];

                    
                    if (!memory.perform_contained(id))
                    {
                        sub_graphs[cur_g].outs.push_back({old_id, id, static_cast<size_t>(memory.find_connection_out_by_in_out(old_id, id))});
                        break;
                    }
                    parents_count[id]++;
                    
                    if (parents_count[id] > 1)
                        break;
                }
            }

        }

        for (size_t i = 0; i < sub_graphs.size(); ++i)
        {
            info.group_info_v[i].all_tasks += sub_graphs[i].tasks.size();
            for (perform_id j: sub_graphs[i].tasks)
            {
                for (perform_id k: memory.get_perform_childs(j))
                {
                    if (perform_group[j] != perform_group[k])
                        sub_graphs[i].outs.push_back({j, k, 0});
                }
            }
        }

        return sub_graphs;
    }

    std::vector<sub_graph> parallelizer2::split_graph_sub(sub_graph& gr, std::map<perform_id, task_graph_node>& mm, exe_info& info)
    {
        std::vector<sub_graph> sub_graphs(info.childs_groups.size());
        std::map<perform_id, size_t> parents_count;
        std::map<perform_id, size_t> perform_group;
        info.all_tasks += gr.tasks.size();

        std::deque<perform_id> ready;
        for (auto& i: gr.tasks)
            if (mm[i].parents_count == 0)
                ready.push_front(i);
        info.all_active_tasks += ready.size();

        std::set<size_t, std::function<bool(size_t, size_t)>> procs([&](size_t a, size_t b)->bool
        {
            double a_d = static_cast<double>(info.group_info_v[a].active_tasks) / info.childs_groups[a].size;
            double b_d = static_cast<double>(info.group_info_v[b].active_tasks) / info.childs_groups[b].size;

            if (a_d != b_d)
                return a_d < b_d;
            return a < b;
        });

        for (size_t i = 0; i < info.childs_groups.size(); ++i)
            procs.insert(i);

        while (ready.size())
        {
            std::queue<perform_id> q;
            q.push(ready.front());
            ready.pop_front();
            while (q.size())
            {
                perform_id id = q.front();
                q.pop();

                size_t cur_g = *procs.begin();
                procs.erase(cur_g);
                ++info.group_info_v[cur_g].active_tasks;
                procs.insert(cur_g);
                
                while (1)
                {
                    sub_graphs[cur_g].tasks.push_back(id);
                    perform_group[id] = cur_g;
                    if (mm[id].childs_v.size() < 1)
                        break;
                    perform_id old_id = id;
                    for (size_t i = 1; i < mm[id].childs_v.size(); ++i)
                    {
                        perform_id tid = mm[id].childs_v[i];
                        parents_count[tid]++;
                        if (parents_count[tid] >= mm[tid].parents_count)
                            q.push(mm[id].childs_v[i]);
                    }
                    id = mm[id].childs_v[0];

                    if (mm.find(id) == mm.end())
                    {
                        sub_graphs[cur_g].outs.push_back({old_id, id, 0});
                        break;
                    }
                    parents_count[id]++;
                    if (parents_count[id] > 1)
                        break;
                }
            }

        }

        for (size_t i = 0; i < sub_graphs.size(); ++i)
        {
            info.group_info_v[i].all_tasks += sub_graphs[i].tasks.size();
            for (perform_id j: sub_graphs[i].tasks)
            {
                for (perform_id k: mm[j].childs_v)
                {
                    if (perform_group[j] != perform_group[k])
                        sub_graphs[i].outs.push_back({j, k, 0});
                }
            }
        }

        return sub_graphs;
    }

    void parallelizer2::transfer_state(const std::vector<sub_graph>& sub_graphs, exe_info& info, std::vector<instruction>& ins_v)
    {
        // 1
        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            ins_v[i].add_transfer_state();
            instr_comm.send<message>(&ins_v[i], info.childs_groups[i].head);
            ins_v[i].clear();
        }

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            ins_v[i].add_task_graph_recv();
            internal_comm.send<message>(&ins_v[i], info.childs_groups[i].internal_head);
            ins_v[i].clear();
        }

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            internal_comm.send(&sub_graphs[i], info.childs_groups[i].internal_head);
            for (perform_id j: sub_graphs[i].tasks)
                memory.send_task_node(j, internal_comm, info.childs_groups[i].internal_head);
        }

        // 2
        std::map<perform_id, process> mm;
        for (size_t i = 0; i < sub_graphs[0].tasks.size(); ++i)
            mm[sub_graphs[0].tasks[i]] = comm.rank();

        for (size_t k = 1; k < sub_graphs.size(); ++k)
        {
            internal_comm.recv<message>(&ins_v[k], info.childs_groups[k].internal_head);

            for (const instruction_block& i: ins_v[k])
            {
                switch (i.command())
                {
                    case INSTRUCTION::PERFORM_ASSIGNED_TO:
                    {
                        const instruction_perform_assigned_to& j = dynamic_cast<const instruction_perform_assigned_to&>(i);
                        mm[j.id()] = j.proc();
                        break;
                    }
                    default:
                        comm.abort(754);
                }
            }
            ins_v[k].clear();
        }

        // 3
        for (size_t i = 0; i < sub_graphs[0].outs.size(); ++i)
        {
            memory.insert_out(sub_graphs[0].outs[i].out, sub_graphs[0].outs[i].in, mm[sub_graphs[0].outs[i].in]);
        }

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            for (size_t j = 0; j < sub_graphs[i].outs.size(); ++j)
                ins_v[i].add_graph_out_proc(sub_graphs[i].outs[j].out, sub_graphs[i].outs[j].in, mm[sub_graphs[i].outs[j].in]);
            internal_comm.send<message>(&ins_v[i], info.childs_groups[i].internal_head);
            ins_v[i].clear();
        }
        
        // 4

        // 5
        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            //for (perform_id j: sub_graphs[i].tasks)
            //    init_task_data_sending(j, i, ins_v.data(), info);
            //internal_comm.send<message>(&ins_v[i], info.childs_groups[i].internal_head);
            
            ins_v[i].clear();
            memory.erase_subgraph(sub_graphs[i], i);
        }

        //std::vector<instruction> r_ins_v(ins_v.size());
        //for (size_t i = 1; i < sub_graphs.size(); ++i)
        //    internal_comm.recv<message>(&r_ins_v[i], info.childs_groups[i].internal_head);

        //match_sendrecv_pairs(r_ins_v, ins_v, info);

        //info.group_workload.assign(info.group_workload.size(), 0);

        // process_instruction(in) self
    }

    void parallelizer2::sub_transfer_state(exe_info& info)
    {
        instruction extern_ins;
        sub_graph extern_graph;
        std::map<perform_id, task_graph_node> extern_task_node;
        std::vector<instruction> ins_v(info.childs_groups.size());
        std::vector<sub_graph> sub_graphs(info.childs_groups.size());

        // 1
        external_comm.recv<message>(&extern_ins, 0);
        if ((*extern_ins.begin()).command() != INSTRUCTION::TASK_GRAPH_RECV)
            comm.abort(755);
        extern_ins.clear();
        external_comm.recv(&extern_graph, 0);
        for (size_t i = 0; i < extern_graph.tasks.size(); ++i)
        {
            task_graph_node tgn;
            external_comm.recv(&tgn, 0);
            extern_task_node[extern_graph.tasks[i]] = tgn;
        }

        // 2
        sub_graphs = std::move(split_graph_sub(extern_graph, extern_task_node, info));

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            ins_v[i].add_transfer_state();
            instr_comm.send<message>(&ins_v[i], info.childs_groups[i].head);
            ins_v[i].clear();
        }

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            ins_v[i].add_task_graph_recv();
            internal_comm.send<message>(&ins_v[i], info.childs_groups[i].internal_head);
            ins_v[i].clear();
        }

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            internal_comm.send(&sub_graphs[i], info.childs_groups[i].internal_head);
            for (perform_id j: sub_graphs[i].tasks)
                internal_comm.send(&extern_task_node[j], info.childs_groups[i].internal_head);
        }

        // 3
        for (auto& i: sub_graphs[0].tasks)
            extern_ins.add_perform_assigned_to(i, comm.rank());

        for (size_t k = 1; k < sub_graphs.size(); ++k)
        {
            internal_comm.recv<message>(&ins_v[k], info.childs_groups[k].internal_head);
            for (const instruction_block& i: ins_v[k])
            {
                switch (i.command())
                {
                    case INSTRUCTION::PERFORM_ASSIGNED_TO:
                    {
                        const instruction_perform_assigned_to& j = dynamic_cast<const instruction_perform_assigned_to&>(i);
                        extern_ins.add_perform_assigned_to(j.id(), j.proc());
                        break;
                    }
                    default:
                        comm.abort(754);
                }
            }
            ins_v[k].clear();
        }
        external_comm.send<message>(&extern_ins, 0);
        extern_ins.clear();

        std::map<std::pair<perform_id, perform_id>, size_t> mmm;
        for (size_t i = 0; i < sub_graphs.size(); ++i)
        {
            for (auto& j: sub_graphs[i].outs)
                mmm[{j.out, j.in}] = i;
        }

        // 4
        external_comm.recv<message>(&extern_ins, 0);
        for (const instruction_block& i: extern_ins)
        {
            switch (i.command())
            {
            case INSTRUCTION::GRAPH_OUT_PROC:
            {
                const instruction_graph_out_proc& j = dynamic_cast<const instruction_graph_out_proc&>(i);
                ins_v[mmm[{j.out(), j.in()}]].add_graph_out_proc(j.out(), j.in(), j.proc());
                break;
            }
            default:
                comm.abort(756);
            }
        }
        extern_ins.clear();

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            internal_comm.send<message>(&ins_v[i], info.childs_groups[i].internal_head);
            ins_v[i].clear();
        }

        for (const instruction_block& i: ins_v[0])
        {
            switch (i.command())
            {
            case INSTRUCTION::GRAPH_OUT_PROC:
            {
                const instruction_graph_out_proc& j = dynamic_cast<const instruction_graph_out_proc&>(i);
                memory.insert_out(j.out(), j.in(), j.proc());
                break;
            }
            default:
                comm.abort(757);
            }
        }
        ins_v[0].clear();

        // 5
        for (auto& i: sub_graphs[0].tasks)
        {
            memory.insert_task_graph_node(i, extern_task_node[i]);
        }

    }

    void parallelizer2::match_sendrecv_pairs(const std::vector<instruction>& ins, std::vector<instruction>& outs, exe_info& info)
    {
        // implementation
    }

    void parallelizer2::init_task_data_sending(perform_id tid, size_t group_id, instruction* inss, exe_info& info)
    {
        for (message_id i: memory.get_perform_data(tid))
            init_message_sending(i, group_id, inss, info);

        for (message_id i: memory.get_perform_const_data(tid))
            init_message_sending(i, group_id, inss, info);

        message_id m_tid = memory.get_task_id(tid).mi;
        init_message_sending(m_tid, group_id, inss, info);
    }

    void parallelizer2::init_message_sending(message_id id, size_t group_id, instruction* inss, exe_info& info)
    {
        std::function<size_t(std::vector<std::set<message_id>>&)> get_group = [&](std::vector<std::set<message_id>>& search_sets)->size_t
        {
            std::set<size_t, std::function<bool(size_t, size_t)>> r_ver([&](size_t a, size_t b)->bool
            {
                float af = static_cast<float>(info.group_workload[a]) / info.childs_groups[a].size;
                float bf = static_cast<float>(info.group_workload[b]) / info.childs_groups[b].size;
                if (af != bf)
                    return af < bf;
                return a < b;
            });

            for (size_t i = 0; i < comm.size(); ++i)
                if (search_sets[i].find(id) != search_sets[i].end())
                    r_ver.insert(i);

            if (r_ver.size())
                return *r_ver.begin();
            return std::numeric_limits<size_t>::max();
        };
        size_t sender_group = std::numeric_limits<size_t>::max();

        if (info.contained_mes[group_id].find(id) == info.contained_mes[group_id].end())
        {
            sender_group = get_group(info.contained_mes);
            if (sender_group == std::numeric_limits<size_t>::max())
                comm.abort(432);

            inss[group_id].add_select_mes_receiver(id);
            inss[sender_group].add_select_mes_sender_with_info(id);

            if (memory.get_message_factory_type(id) == MESSAGE_FACTORY_TYPE::CHILD)
                inss[group_id].add_message_part_creation(id, memory.get_message_type(id), memory.get_message_parent(id), info.childs_groups[sender_group].head);
            else
                inss[group_id].add_message_creation(id, memory.get_message_type(id), info.childs_groups[sender_group].head);
            info.contained_mes[group_id].insert(id);

            ++info.group_workload[group_id];
            ++info.group_workload[sender_group];
        }
        if (info.versions_mes[group_id].find(id) == info.versions_mes[group_id].end())
        {
            sender_group = get_group(info.versions_mes);
            if (sender_group == MPI_PROC_NULL)
            {
                std::set<message_id>& ch = memory.get_message_childs(id);
                if (ch.size() == 0)
                    comm.abort(432);
                for (message_id i: ch)
                {
                    init_message_sending(i, group_id, inss, info);
                    inss[group_id].add_include_child_to_parent(id, i);
                    memory.set_message_child_state(i, CHILD_STATE::INCLUDED);
                }
            }
            else
            {
                if ((info.contained_mes[group_id].find(memory.get_message_parent(id)) == info.contained_mes[group_id].end()) || (info.versions_mes[group_id].find(memory.get_message_parent(id)) == info.versions_mes[group_id].end()))
                {
                    inss[group_id].add_message_receiving(id, info.childs_groups[sender_group].head);
                    inss[sender_group].add_message_sending(id, info.childs_groups[group_id].head);

                    ++info.group_workload[group_id];
                    ++info.group_workload[sender_group];
                }
            }
            info.versions_mes[group_id].insert(id);
        }
    }

    bool parallelizer2::process_instruction(instruction& ins, exe_info& info, process assigner)
    {
        bool ret = true;
        for (const instruction_block& i: ins)
        {
            switch (i.command())
            {
                case INSTRUCTION::END:
                {
                   ret = false;
                   break;
                }
                case INSTRUCTION::SIGN_GRAPH_OUT:
                {
                    const instruction_sign_graph_out& j = dynamic_cast<const instruction_sign_graph_out&>(i);
                    if (memory.perform_contained(j.in()))
                    {
                        memory.set_perform_parents_count(j.in(), memory.get_perform_parents_count(j.in()) - 1);
                        if (memory.get_perform_parents_count(j.in()) == 0)
                        {
                            info.ready_tasks.push_back(j.in());
                            ++info.all_active_tasks;
                            ++info.group_info_v[0].active_tasks;
                        }
                    }
                    break;
                }
                case INSTRUCTION::SIGN_GRAPH_CHILD_OUT:
                {
                    const instruction_sign_graph_child_out& j = dynamic_cast<const instruction_sign_graph_child_out&>(i);
                    memory.connect_extern_child_out(j.out(), j.in(), assigner);
                    update_ready_tasks(j.out(), info);
                    break;
                }
                case INSTRUCTION::TRANSFER_STATE:
                {
                    sub_transfer_state(info);
                    info.ready_tasks = memory.get_ready_tasks();
                    break;
                }
                case INSTRUCTION::SEND_EXE_STATS:
                {
                    const instruction_send_exe_stats& j = dynamic_cast<const instruction_send_exe_stats&>(i);
                    size_t ext_group = info.groups_of_processes[assigner];
                    info.all_active_tasks -= info.group_info_v[ext_group].active_tasks;
                    info.group_info_v[ext_group].active_tasks = j.active_count();
                    info.all_active_tasks += info.group_info_v[ext_group].active_tasks;
                    info.all_tasks -= info.group_info_v[ext_group].all_tasks;
                    info.group_info_v[ext_group].all_tasks = j.all_count();
                    info.all_tasks += info.group_info_v[ext_group].all_tasks;
                    break;
                }
                default:
                    comm.abort(555);
            }
        }
        return ret;
    }

    void parallelizer2::execute_task(perform_id id, exe_info& info)
    {
        task_environment env(memory.get_perform_type(id));
        env.set_proc_count(comm.size());
        memory.perform_task(id, env);

        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_perform_data(id))
        {
            for (size_t k = 0; k < info.internal_comm_size; ++k)
                info.versions_mes[k].erase(i);
            info.versions_mes[0].insert(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                j = memory.get_message_parent(j);
                for (size_t k = 0; k < info.internal_comm_size; ++k)
                    info.versions_mes[k].erase(j);
            }
        }

        message_id t_mes_id = memory.get_task_id(id).mi;

        for (size_t k = 0; k < info.internal_comm_size; ++k)
            info.versions_mes[k].erase(t_mes_id);
        info.versions_mes[0].insert(t_mes_id);

        for (const local_message_id& i: env.result_message_ids())
        {
            switch (i.src)
            {
            case MESSAGE_SOURCE::INIT:
            {
                message_init_data& d = env.created_messages_init()[i.id];
                messages_init_id.push_back(memory.create_message_init(d.type, d.ii, comm.rank()));
                info.contained_mes[0].insert(messages_init_id.back());
                info.versions_mes[0].insert(messages_init_id.back());
                break;
            }
            case MESSAGE_SOURCE::INIT_A:
            {
                message_init_add_data& d = env.added_messages_init()[i.id];
                messages_init_add_id.push_back(memory.add_message_init(d.mes, d.type, d.ii, comm.rank()));
                info.contained_mes[0].insert(messages_init_add_id.back());
                info.versions_mes[0].insert(messages_init_add_id.back());
                break;
            }
            case MESSAGE_SOURCE::CHILD:
            {
                message_child_data& d = env.created_messages_child()[i.id];
                message_id src {};
                switch (d.sourse.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    {
                        src = memory.get_perform_data(id)[d.sourse.id];
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        src = memory.get_perform_const_data(id)[d.sourse.id];
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
                        comm.abort(767);
                }
                messages_childs_id.push_back(memory.create_message_child(d.type, src, d.pi, comm.rank()));
                info.contained_mes[0].insert(messages_childs_id.back());
                info.versions_mes[0].insert(messages_childs_id.back());
                break;
            }
            case MESSAGE_SOURCE::CHILD_A:
            {
                message_child_add_data& d = env.added_messages_child()[i.id];
                message_id src {};
                switch (d.sourse.src)
                {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    src = memory.get_perform_data(id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    src = memory.get_perform_const_data(id)[d.sourse.id];
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
                    comm.abort(767);
                }
                for (size_t k = 1; k < info.internal_comm_size; ++k)
                    info.versions_mes[k].erase(src);
                messages_childs_add_id.push_back(memory.add_message_child(d.mes, d.type, src, d.pi, comm.rank()));
                info.contained_mes[0].insert(messages_childs_add_id.back());
                info.versions_mes[0].insert(messages_childs_add_id.back());
                break;
            }
            default:
                comm.abort(767);
            }
        }

        size_t id_childs = 0;
        for (const local_task_id& i: env.result_task_ids())
        {
            message_id mes_t_id {};
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
                    ++id_childs;
                    break;
                }
                default:
                    comm.abort(767);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    mes_t_id = memory.get_perform_data(id)[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = memory.get_perform_const_data(id)[i.mes.id];
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
                default:
                    comm.abort(767);
            }

            std::vector<message_id> data_id;
            data_id.reserve(local_data.size());
            for (local_message_id k: local_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    {
                        data_id.push_back(memory.get_perform_data(id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(memory.get_perform_const_data(id)[k.id]);
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
                        comm.abort(767);
                }
            }

            std::vector<message_id> const_data_id;
            const_data_id.reserve(local_c_data.size());
            for (local_message_id k: local_c_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    {
                        const_data_id.push_back(memory.get_perform_data(id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(memory.get_perform_const_data(id)[k.id]);
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
                        comm.abort(767);
                }
            }

            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    perform_id tid = memory.add_perform(mes_t_id, t.type, data_id, const_data_id, comm.rank());
                    tasks_id.push_back({mes_t_id, tid});
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    perform_id tid = memory.add_perform(mes_t_id, t.type, data_id, const_data_id, comm.rank());
                    tasks_child_id.push_back({mes_t_id, tid});
                    memory.set_task_parent({mes_t_id, tid}, memory.get_task_id(id));
                    break;
                }
                default:
                    comm.abort(767);
            }
        }
        memory.set_perform_created_childs(id, memory.get_perform_created_childs(id) + id_childs);

        for (const task_dependence& i: env.created_dependences())
        {
            perform_id parent {};
            perform_id child {};
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
                    parent = id;
                    break;
                }
                default:
                    comm.abort(767);
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
                    child = id;
                    break;
                }
                default:
                    comm.abort(767);
            }
            memory.add_dependence(parent, child);
        }

        for (const task_id& i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push_front(i.pi);
        }

        for (const task_id& i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push_front(i.pi);
        }

        update_ready_tasks(id, info);
    }

    void parallelizer2::update_ready_tasks(perform_id tid, exe_info& info)
    {
        perform_id c_t = tid;
        instruction result_instruction;
        while (1)
        {
            if (memory.get_perform_created_childs(c_t) == 0)
            {
                for (perform_id i: memory.get_perform_childs(c_t))
                {
                    if (memory.perform_contained(i))
                    {
                        memory.set_perform_parents_count(i, memory.get_perform_parents_count(i) - 1);
                        if (memory.get_perform_parents_count(i) == 0)
                        {
                            info.ready_tasks.push_front(i);
                            ++info.all_active_tasks;
                            ++info.group_info_v[0].active_tasks;
                        }
                    }
                    else
                    {
                        process proc = memory.find_connection_out_by_in_out(c_t, i);
                        memory.erase_out(c_t, i, proc);
                        result_instruction.add_sign_graph_out(c_t, i);
                        instr_comm.send<message>(&result_instruction, proc);
                        result_instruction.clear();
                    }
                }
            }
            else
                break;
            if (!memory.perform_has_parent(c_t))
            {
                memory.delete_perform(c_t);
                break;
            }
            else
            {
                perform_id old_c_t = c_t;
                c_t = memory.get_perform_parent(c_t);
                memory.delete_perform(old_c_t);
                if (memory.perform_contained(c_t))
                    memory.set_perform_created_childs(c_t, memory.get_perform_created_childs(c_t) - 1);
                else
                {
                    process proc = memory.find_connection_child_in_by_in_out(c_t, old_c_t);
                    memory.erase_child_in(c_t, old_c_t, proc);
                    result_instruction.add_sign_graph_child_out(c_t, old_c_t);
                    instr_comm.send<message>(&result_instruction, proc);
                    result_instruction.clear();
                    break;
                }
            }
        }
    }

    process parallelizer2::get_current_proc()
    {
        return comm.rank();
    }

    int parallelizer2::get_proc_count()
    {
        return comm.size();
    }

    void parallelizer2::clear()
    {
        memory.clear();
    }

}
