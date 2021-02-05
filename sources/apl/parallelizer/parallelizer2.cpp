#include "apl/parallelizer/parallelizer2.h"

namespace apl
{

    const process parallelizer2::main_proc = 0;

    parallelizer2::parallelizer2(const intracomm& _comm): comm(_comm), instr_comm(comm)
    { }

    parallelizer2::parallelizer2(task_graph& _tg, const intracomm& _comm): comm(_comm), instr_comm(comm), memory(_tg)
    { }

    parallelizer2::~parallelizer2()
    { }

    void parallelizer2::init(task_graph& _tg)
    {
        memory.init(_tg);
    }

    void parallelizer2::execution(task_graph& _tg)
    {
        init(_tg);
        execution(2);
    }

    void parallelizer2::execution(size_t tree_param)
    {
        std::vector<std::set<message_id>> versions(comm.size());
        versions[0] = memory.get_messages_set();
        for (process i = 1; i < comm.size(); ++i)
            versions[i] = versions[0];

        std::vector<std::set<message_id>> contained(comm.size());
        for (process i = 0; i < comm.size(); ++i)
            contained[i] = versions[0];

        std::vector<std::set<perform_id>> contained_tasks(comm.size());
        contained_tasks[0] = memory.get_performs_set();
        for (process i = 1; i < comm.size(); ++i)
            contained_tasks[i] = contained_tasks[0];

        memory_manager_graph_adapter adapter(memory);
        graph_analizer graph(adapter);

        std::vector<processes_group> groups = std::move(make_topology(tree_param));

        process parent = groups[comm.rank()].owner;
        std::vector<processes_group> childs_groups;
        childs_groups.push_back({ 1, comm.rank(), MPI_PROC_NULL });
        for (processes_group& i : groups)
            if (i.owner == comm.rank())
                childs_groups.push_back(i);
        groups.clear();

        std::deque<perform_id> ready_tasks;
        std::vector<instruction> internal_instructions(childs_groups.size());
        instruction external_instruction;

        if (parent == MPI_PROC_NULL)
        {

        }

        bool running = true;
        while (running)
        {

            if (parent == MPI_PROC_NULL)
            {

            }
            else
            {
                process new_instr_process = MPI_PROC_NULL;
                if (ready_tasks.empty())
                    new_instr_process = instr_comm.wait_any_process();
                else
                    new_instr_process = instr_comm.test_any_process();

                if (new_instr_process != MPI_PROC_NULL)
                {
                    instr_comm.recv<instruction>(&external_instruction, new_instr_process);
                }
            }

            running = process_instruction(external_instruction);
            external_instruction.clear();

            if (ready_tasks.size())
            {
                execute_task(ready_tasks.front());
                ready_tasks.pop_front();
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

    void parallelizer2::send_task_data(perform_id tid, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
    {
        for (message_id i : memory.get_perform_data(tid))
            send_message(i, proc, inss, ver, con);

        for (message_id i : memory.get_perform_const_data(tid))
            send_message(i, proc, inss, ver, con);

        message_id m_tid = memory.get_task_id(tid).mi;
        send_message(m_tid, proc, inss, ver, con);
    }

    void parallelizer2::send_message(message_id id, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
    {
        std::function<process(std::vector<std::set<message_id>>&)> get_proc = [&](std::vector<std::set<message_id>>& search_sets)->process
        {
            std::set<process, std::function<bool(process, process)>> r_ver([&](process a, process b)->bool
                {
                    if (comm_workload[a] != comm_workload[b])
                        return comm_workload[a] < comm_workload[b];
                    return a < b;
                });

            for (process i = 0; i < comm.size(); ++i)
            {
                if (search_sets[i].find(id) != search_sets[i].end())
                {
                    r_ver.insert(i);
                }
            }
            if (r_ver.size())
                return *r_ver.begin();
            return MPI_PROC_NULL;
        };
        process sender_proc = MPI_PROC_NULL;

        if (con[proc].find(id) == con[proc].end())
        {
            sender_proc = get_proc(con);
            if (sender_proc == MPI_PROC_NULL)
                comm.abort(432);

            if (memory.get_message_factory_type(id) == MESSAGE_FACTORY_TYPE::CHILD)
                inss[proc].add_message_part_creation(id, memory.get_message_type(id), memory.get_message_parent(id), sender_proc);
            else
                inss[proc].add_message_creation(id, memory.get_message_type(id), sender_proc);
            con[proc].insert(id);
            inss[sender_proc].add_message_info_sending(id, proc);

            ++comm_workload[proc];
            ++comm_workload[sender_proc];
        }
        if (ver[proc].find(id) == ver[proc].end())
        {
            sender_proc = get_proc(ver);
            if (sender_proc == MPI_PROC_NULL)
            {
                std::set<message_id>& ch = memory.get_message_childs(id);
                if (ch.size() == 0)
                    comm.abort(432);
                for (message_id i : ch)
                {
                    send_message(i, proc, inss, ver, con);
                    inss[proc].add_include_child_to_parent(id, i);
                    memory.set_message_child_state(i, CHILD_STATE::INCLUDED);
                }
            }
            else
            {
                if ((con[proc].find(memory.get_message_parent(id)) == con[proc].end()) || (ver[proc].find(memory.get_message_parent(id)) == ver[proc].end()))
                {
                    inss[proc].add_message_receiving(id, sender_proc);
                    inss[sender_proc].add_message_sending(id, proc);

                    ++comm_workload[proc];
                    ++comm_workload[sender_proc];
                }
            }
            ver[proc].insert(id);
        }
    }

    void parallelizer2::assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<perform_id>>& com)
    {
        if (com[proc].find(tid.pi) == com[proc].end())
        {
            ins.add_task_creation(tid, memory.get_perform_type(tid.pi), memory.get_task_data(tid), memory.get_task_const_data(tid));
            com[proc].insert(tid.pi);
        }
        ins.add_task_execution(tid);
    }

    bool parallelizer2::process_instruction(instruction& ins)
    {
        for (const instruction_block& i : ins)
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
                for (message* p : memory.get_message_info(j.id()))
                    comm.isend(p, j.proc(), info_req);
                break;
            }
            case INSTRUCTION::MES_CREATE:
            {
                const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                request_block info_req;
                std::vector<message*> iib = message_init_factory::get_info(j.type());
                for (message* p : iib)
                    comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_init_with_id(j.id(), j.type(), iib);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                std::vector<message*> pib = message_child_factory::get_info(j.type());
                request_block info_req;
                for (message* p : pib)
                    comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_child_with_id(j.id(), j.type(), j.source(), pib);
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
                ready_tasks.push(j.id().pi);
                break;
            }
            case INSTRUCTION::TASK_CREATE:
            {
                const instruction_task_create& j = dynamic_cast<const instruction_task_create&>(i);
                memory.add_perform_with_id(j.id(), j.type(), j.data(), j.const_data());
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

    void parallelizer2::execute_task(perform_id id)
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
