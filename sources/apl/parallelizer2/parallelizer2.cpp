#include "apl/parallelizer2/parallelizer2.h"

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
        
            info.childs_groups.push_back({1, comm.rank(), MPI_PROC_NULL});
            for (processes_group& i: groups)
                if (i.owner == comm.rank())
                    info.childs_groups.push_back(i);


        }

        info.group_info_v.resize(info.childs_groups.size());
        info.group_workload.resize(info.childs_groups.size());

        info.versions_mes.resize(info.childs_groups.size());
        info.versions_mes[0] = memory.get_messages_set();
        for (process i = 1; i < comm.size(); ++i)
            info.versions_mes[i] = info.versions_mes[0];

        info.contained_mes.resize(info.childs_groups.size());
        for (process i = 0; i < comm.size(); ++i)
            info.contained_mes[i] = info.versions_mes[0];

        info.contained_tasks.resize(info.childs_groups.size());
        info.contained_tasks[0] = memory.get_performs_set();
        for (process i = 1; i < comm.size(); ++i)
            info.contained_tasks[i] = info.contained_tasks[0];

        std::vector<instruction> internal_instructions(info.childs_groups.size());
        instruction external_instruction;

        if (comm.rank() == main_proc)
        {
            info.ready_tasks = memory.get_ready_tasks(main_proc);
            info.group_info_v[0].active_tasks = info.ready_tasks.size();
        }

        bool running = true;
        while (running)
        {
            if ((info.active_graphs == 0) && (graph.need_split(info.childs_groups, info.group_info_v, info.ready_tasks.size())))
            {
                   std::vector<sub_graph> tasks_for_childs = graph.split_graph(info.childs_groups, info.group_info_v, info.ready_tasks);

                   generate_subgraph_instructions(tasks_for_childs, info, internal_instructions);
            }

            process ext_proc = MPI_PROC_NULL;
            while ((ext_proc = instr_comm.test_any_process()) != MPI_PROC_NULL)
            {
                instr_comm.recv(&external_instruction, ext_proc);
                if (!process_instruction(external_instruction, info, ext_proc))
                    running = false;
            }

            if (info.ready_tasks.size())
            {
                perform_id up_id = info.ready_tasks.front();
                info.ready_tasks.pop_front();
                execute_task(up_id, info);
                if (info.ready_tasks.empty())
                    --info.active_graphs;
            }

            if ((info.parent == MPI_PROC_NULL) && (info.active_graphs == 0) && (memory.task_count() == 0))
            {
                for (size_t i = 1; i < info.group_info_v.size(); ++i)
                {
                    internal_instructions[i].add_end();
                    instr_comm.send<instruction>(&internal_instructions[i], info.childs_groups[i].head);
                }
                running = false;
            }
        }

        clear();
    }

    std::vector<processes_group> parallelizer2::make_topology(size_t tree_param)
    {
        std::vector<processes_group> groups(comm.size());
        std::queue<process> proc_q;
        proc_q.push(0);
        groups[0].owner = MPI_PROC_NULL;
        process cur_proc = 1;
        while (cur_proc < comm.size())
        {
            process next = proc_q.front();
            proc_q.pop();
            for (size_t i = 0; (i < tree_param) && (cur_proc < comm.size()); ++i)
            {
                groups[cur_proc].owner = next;
                proc_q.push(cur_proc);
                ++cur_proc;
            }
            groups[next].head = next;
            groups[next].size = 1;
        }
        for (size_t j = groups.size() - 1; j > 0; --j)
        {
            groups[groups[j].owner].size += groups[j].size;
        }
        return groups;
    }

    void parallelizer2::generate_subgraph_instructions(const std::vector<sub_graph>& sub_graphs, exe_info& info, std::vector<instruction>& ins_v)
    {
        if (sub_graphs[0].tasks.size() > 0)
            ++info.active_graphs;

        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            if (sub_graphs[i].tasks.size() > 0)
                ++info.active_graphs;
            for (perform_id j: sub_graphs[i].tasks)
                init_task_data_sending(j, i, ins_v.data(), info);
        }



        for (size_t i = 1; i < sub_graphs.size(); ++i)
        {
            ins_v[i].add_task_graph_recv();
            instr_comm.send(&ins_v[i], info.childs_groups[i].head);
            ins_v[i].clear();
            instr_comm.send(&sub_graphs[i], info.childs_groups[i].head);
            for (perform_id j: sub_graphs[i].tasks)
                memory.send_task_node(j, instr_comm, info.childs_groups[i].head);
            memory.erase_subgraph(sub_graphs[i], i);
        }
        info.group_workload.assign(info.group_workload.size(), 0);
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
            info.contained_mes[group_id].insert(id);
            inss[sender_group].add_select_mes_sender(id);

            if (memory.get_message_factory_type(id) == MESSAGE_FACTORY_TYPE::CHILD)
                inss[group_id].add_message_part_creation(id, memory.get_message_type(id), memory.get_message_parent(id), info.childs_groups[sender_group].head);
            else
                inss[group_id].add_message_creation(id, memory.get_message_type(id), info.childs_groups[sender_group].head);
            info.contained_mes[group_id].insert(id);
            inss[sender_group].add_message_info_sending(id, info.childs_groups[group_id].head);

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
        for (const instruction_block& i: ins)
        {
            switch (i.command())
            {
            case INSTRUCTION::END:
                return false;
            case INSTRUCTION::MES_SEND:
            {
                const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                memory.send_message(j.id(), comm, j.proc());
                break;
            }
            case INSTRUCTION::MES_RECV:
            {
                const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                memory.recv_message(j.id(), comm, j.proc());
                break;
            }
            case INSTRUCTION::MES_INFO_SEND:
            {
                const instruction_message_info_send& j = dynamic_cast<const instruction_message_info_send&>(i);
                request_block& info_req = memory.get_message_info_request_block(j.id());
                for (message* p: memory.get_message_info(j.id()))
                    comm.isend(p, j.proc(), info_req);
                break;
            }
            case INSTRUCTION::MES_CREATE:
            {
                const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                request_block info_req;
                std::vector<message*> iib = message_init_factory::get_info(j.type());
                for (message* p: iib)
                    comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_init_with_id(j.id(), j.type(), iib, assigner);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                std::vector<message*> pib = message_child_factory::get_info(j.type());
                request_block info_req;
                for (message* p: pib)
                    comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_child_with_id(j.id(), j.type(), j.source(), pib, assigner);
                break;
            }
            case INSTRUCTION::INCLUDE_MES_CHILD:
            {
                const instruction_message_include_child_to_parent& j = dynamic_cast<const instruction_message_include_child_to_parent&>(i);
                memory.include_child_to_parent(j.child());
                break;
            }
            case INSTRUCTION::TASK_EXE:
            {
                const instruction_task_execute& j = dynamic_cast<const instruction_task_execute&>(i);
                info.ready_tasks.push_back(j.id().pi);
                break;
            }
            case INSTRUCTION::TASK_CREATE:
            {
                const instruction_task_create& j = dynamic_cast<const instruction_task_create&>(i);
                memory.add_perform_with_id(j.id(), j.type(), j.data(), j.const_data(), assigner);
                break;
            }
            case INSTRUCTION::TASK_RES:
            {
                comm.abort(876);
            }
            case INSTRUCTION::ADD_RES_TO_MEMORY:
            {
                comm.abort(876);
            }
            case INSTRUCTION::MES_DEL:
            {
                const instruction_message_delete& j = dynamic_cast<const instruction_message_delete&>(i);
                memory.delete_message(j.id());
                break;
            }
            case INSTRUCTION::TASK_DEL:
            {
                const instruction_task_delete& j = dynamic_cast<const instruction_task_delete&>(i);
                memory.delete_perform(j.id());
                break;
            }
            }
        }
        return true;
    }

    void parallelizer2::execute_task(perform_id id, exe_info& info)
    {
        task_environment env(memory.get_perform_type(id));
        env.set_proc_count(comm.size());
        memory.perform_task(id, env);


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
