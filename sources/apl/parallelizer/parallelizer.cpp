#include "apl/parallelizer/parallelizer.h"

namespace apl
{

    const process parallelizer::main_proc = 0;

    parallelizer::parallelizer(size_t thread_count, const intracomm& _comm): comm(_comm), instr_comm(comm), execution_thread_count(thread_count)
    { }

    parallelizer::parallelizer(task_graph& _tg, size_t thread_count, const intracomm& _comm): comm(_comm), instr_comm(comm), memory(_tg), execution_thread_count(thread_count)
    { }

    parallelizer::~parallelizer()
    { }

    void parallelizer::init(task_graph& _tg)
    { memory.init(_tg); }

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
        std::vector<std::set<message_id>> versions_of_messages(comm.size());
        versions_of_messages[0] = memory.get_messages_set();
        for (process i = 1; i < comm.size(); ++i)
            versions_of_messages[i] = versions_of_messages[0];

        std::vector<std::set<message_id>> contained_messages(comm.size());
        for (process i = 0; i < comm.size(); ++i)
            contained_messages[i] = versions_of_messages[0];

        std::vector<std::set<perform_id>> contained_tasks(comm.size());
        contained_tasks[0] = memory.get_performs_set();
        for (process i = 1; i < comm.size(); ++i)
            contained_tasks[i] = contained_tasks[0];

        std::vector<instruction> ins(comm.size());
        std::vector<std::vector<perform_id>> assigned(comm.size());
        size_t all_assigned = 0;

        std::vector<std::unique_ptr<std::thread>> task_execution_thread_v(execution_thread_count - 1);
        for (auto& i: task_execution_thread_v)
            i.reset(new std::thread(&parallelizer::task_execution_thread_function, this, comm.size()));

        ready_tasks = std::move(memory.get_ready_tasks());

        while (ready_tasks.size())
        {

            for (perform_id i: tasks_to_del)
            {
                contained_tasks[0].erase(i);
                memory.delete_perform(i);
                for (process j = 1; j < comm.size(); ++j)
                {
                    if (contained_tasks[j].find(i) != contained_tasks[j].end())
                    {
                        contained_tasks[j].erase(i);
                        ins[j].add_task_del(i);
                    }
                }
            }
            while (memory.get_unreferenced_messages().size())
            {
                message_id i = *memory.get_unreferenced_messages().begin();
                memory.delete_message_from_graph(i);

                for (process j = 0; j < comm.size(); ++j)
                {
                    if (contained_messages[j].find(i) != contained_messages[j].end())
                    {
                        contained_messages[j].erase(i);
                        versions_of_messages[j].erase(i);
                        ins[j].add_message_del(i);
                    }
                }
            }
            tasks_to_del.clear();

            {
                size_t comm_size_quotient = ready_tasks.size() / comm.size();
                size_t comm_size_remainder = ready_tasks.size() % comm.size();
                for (process i = 0; i < comm.size(); ++i)
                {
                    size_t max_assigned_tasks_count = comm_size_quotient + ((i < comm_size_remainder) ? 1 : 0);
                    assigned[i].resize(max_assigned_tasks_count);
                    assigned[i].assign(max_assigned_tasks_count, PERFORM_ID_UNDEFINED);
                }
                std::vector<size_t> assigned_current_position(comm.size(), 0);

                comm_workload.resize(comm.size());
                while (ready_tasks.size())
                {
                    perform_id current_id = ready_tasks.front();
                    ready_tasks.pop();

                    std::vector<process> decision_vector;
                    std::vector<double> contained_task_data_counts(comm.size(), 0.0);
                    std::vector<message_id>& cur_task_data_ids = memory.get_perform_data(current_id);
                    std::vector<message_id>& cur_task_const_data_ids = memory.get_perform_const_data(current_id);
                    for (process i = 0; i < comm.size(); ++i)
                    {
                        if (assigned_current_position[i] < assigned[i].size())
                        {
                            decision_vector.push_back(i);
                            for (auto j: cur_task_data_ids)
                            {
                                size_t depth = 0;
                                message_id current = j;
                                while ((current != MESSAGE_ID_UNDEFINED) && (depth < comm.size()))
                                {
                                    if (versions_of_messages[i].find(current) != versions_of_messages[i].end())
                                    {
                                        contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(comm.size());
                                        break;
                                    }
                                    ++depth;
                                    current = memory.get_message_parent(current);
                                }
                            }
                            for (auto j: cur_task_const_data_ids)
                            {
                                size_t depth = 0;
                                message_id current = j;
                                while (current != MESSAGE_ID_UNDEFINED)
                                {
                                    if (versions_of_messages[i].find(current) != versions_of_messages[i].end())
                                    {
                                        contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(comm.size());
                                        break;
                                    }
                                    ++depth;
                                    current = memory.get_message_parent(current);
                                }
                            }
                        }
                    }

                    std::sort(decision_vector.begin(), decision_vector.end(), [&](process a, process b)->bool
                    {
                        if (contained_task_data_counts[a] != contained_task_data_counts[b])
                            return contained_task_data_counts[a] > contained_task_data_counts[b];
                        return a > b;
                    });

                    process current_proc = decision_vector[0];
                    assigned[current_proc][assigned_current_position[current_proc]++] = current_id;
                    send_task_data(current_id, current_proc, ins.data(), versions_of_messages, contained_messages);
                    ++all_assigned;
                }
                comm_workload.clear();
            }

            for (process i = 1; i < comm.size(); ++i)
                for (perform_id j: assigned[i])
                    assign_task(memory.get_task_id(j), i, ins[i], contained_tasks);

            for (process i = 1; i < comm.size(); ++i)
            {
                if (ins[i].size() > 0)
                {
                    instr_comm.send<message>(&ins[i], i);
                    ins[i].clear();
                }
            }
            send_instruction(ins[0]);
            ins[0].clear();

            for (perform_id i: assigned[0])
            {
                task_execution_queue_data current_data {i, memory.get_task(memory.get_task_id(i).mi), memory.get_perform_type(i)};
                for (message_id j: memory.get_perform_data(i))
                    current_data.args.push_back(memory.get_message(j));
                for (message_id j: memory.get_perform_const_data(i))
                    current_data.const_args.push_back(memory.get_message(j));

                task_queue_mutex.lock();
                task_queue.push(std::move(current_data));
                task_queue_mutex.unlock();
            }

            while ((all_assigned > 0) && (assigned[0].size() > 0))
            {
                bool queue_try = false;
                bool comm_try = false;
                
                queue_try = finished_task_queue_mutex.try_lock();
                if (queue_try)
                {
                    if (finished_task_queue.empty())
                    {
                        queue_try = false;
                        finished_task_queue_mutex.unlock();
                    }
                    else
                    {
                        finished_task_execution_queue_data current_finished_task_data {finished_task_queue.front()};
                        finished_task_queue.pop();
                        finished_task_queue_mutex.unlock();

                        end_main_task(current_finished_task_data.this_task_id, current_finished_task_data.this_task_environment, versions_of_messages, contained_messages, contained_tasks);
                        assigned[0].pop_back();
                        --all_assigned;
                    }
                }

                process current_proc = instr_comm.test_any_process();
                if (current_proc != MPI_PROC_NULL)
                {
                    comm_try = true;
                    wait_task(current_proc, versions_of_messages, contained_messages, contained_tasks);
                    --all_assigned;
                }

                if (!comm_try && !queue_try)
                {
                    task_queue_mutex.lock();
                    if (task_queue.empty())
                    {
                        task_queue_mutex.unlock();
                        std::this_thread::yield();
                        continue;
                    }

                    task_execution_queue_data current_execution_data{ task_queue.front() };
                    task_queue.pop();
                    task_queue_mutex.unlock();

                    finished_task_execution_queue_data current_output_data{ current_execution_data.this_task_id, {current_execution_data.task_type,
                        current_execution_data.args.size(), current_execution_data.const_args.size(), static_cast<size_t>(comm.size()), execution_thread_count} };
                    current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
                    task_factory::perform(current_execution_data.task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
                    current_execution_data.this_task->set_environment(nullptr);

                    finished_task_queue_mutex.lock();
                    finished_task_queue.push(std::move(current_output_data));
                    finished_task_queue_mutex.unlock();
                }
            }

            while (all_assigned > 0)
            {
                process current_proc = instr_comm.wait_any_process();
                wait_task(current_proc, versions_of_messages, contained_messages, contained_tasks);
                --all_assigned;
            }

            for (auto& i: assigned)
            {
                i.clear();
            }

        }

        instruction end;
        end.add_end();
        for (process i = 1; i < instr_comm.size(); ++i)
            instr_comm.send<message>(&end, i);

        task_queue_mutex.lock();
        task_execution_queue_data exe_thread_end_data;
        exe_thread_end_data.this_task = nullptr;
        task_queue.push(exe_thread_end_data);
        task_queue_mutex.unlock();
        for (auto& i: task_execution_thread_v)
            i->join();
    }

    void parallelizer::task_execution_thread_function(size_t processes_count)
    {
        while (true)
        {
            task_queue_mutex.lock();
            if (task_queue.empty())
            {
                task_queue_mutex.unlock();
                std::this_thread::yield();
                continue;
            }
            task_execution_queue_data current_execution_data{task_queue.front()};
            task_queue.pop();
            task_queue_mutex.unlock();

            if (current_execution_data.this_task == nullptr)
                break;

            finished_task_execution_queue_data current_output_data {current_execution_data.this_task_id, {current_execution_data.task_type,
                current_execution_data.args.size(), current_execution_data.const_args.size(), processes_count, execution_thread_count}};
            current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
            task_factory::perform(current_execution_data.task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
            current_execution_data.this_task->set_environment(nullptr);

            finished_task_queue_mutex.lock();
            finished_task_queue.push(std::move(current_output_data));
            finished_task_queue_mutex.unlock();
        }

        task_queue_mutex.lock();
        task_execution_queue_data exe_thread_end_data;
        exe_thread_end_data.this_task = nullptr;
        task_queue.push(exe_thread_end_data);
        task_queue_mutex.unlock();
    }

    void parallelizer::send_task_data(perform_id tid, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
    {
        for (message_id i: memory.get_perform_data(tid))
            send_message(i, proc, inss, ver, con);

        for (message_id i: memory.get_perform_const_data(tid))
            send_message(i, proc, inss, ver, con);

        message_id m_tid = memory.get_task_id(tid).mi;
        send_message(m_tid, proc, inss, ver, con);
    }

    void parallelizer::send_message(message_id id, process proc, instruction* inss, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con)
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
                for (message_id i: ch)
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

    void parallelizer::assign_task(task_id tid, process proc, instruction& ins, std::vector<std::set<perform_id>>& com)
    {
        if (com[proc].find(tid.pi) == com[proc].end())
        {
            ins.add_task_creation(tid, memory.get_perform_type(tid.pi), memory.get_task_data(tid), memory.get_task_const_data(tid));
            com[proc].insert(tid.pi);
        }
        ins.add_task_execution(tid);
    }

    void parallelizer::send_instruction(instruction& ins)
    {
        for (const instruction_block& i: ins)
        {
            switch (i.command())
            {
            case INSTRUCTION::MES_SEND:
            {
                const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                comm.isend(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
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
            case INSTRUCTION::MES_RECV:
            {
                const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                comm.irecv(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
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
                memory.create_message_init_with_id(j.id(), j.type(), iib);
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
                memory.create_message_child_with_id(j.id(), j.type(), j.source(), pib);
                break;
            }
            case INSTRUCTION::INCLUDE_MES_CHILD:
            {
                const instruction_message_include_child_to_parent& j = dynamic_cast<const instruction_message_include_child_to_parent&>(i);
                memory.include_child_to_parent(j.child());
                break;
            }
            case INSTRUCTION::MES_DEL:
            {
                const instruction_message_delete& j = dynamic_cast<const instruction_message_delete&>(i);
                memory.delete_message(j.id());
                break;
            }
            case INSTRUCTION::TASK_CREATE:
            case INSTRUCTION::TASK_EXE:
            case INSTRUCTION::TASK_DEL:

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
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                j = memory.get_message_parent(j);
                for (process k = 0; k < comm.size(); ++k)
                    ver[k].erase(j);
            }
        }

        message_id t_mes_id = memory.get_task_id(tid).mi;

        for (process k = 0; k < comm.size(); ++k)
            ver[k].erase(t_mes_id);
        ver[main_proc].insert(t_mes_id);

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
                    message_id src {};
                    switch (d.sourse.src)
                    {
                        case MESSAGE_SOURCE::TASK_ARG:
                        {
                            src = memory.get_perform_data(tid)[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = memory.get_perform_const_data(tid)[d.sourse.id];
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
                    messages_childs_id.push_back(memory.create_message_child(d.type, src, d.pi));
                    con[main_proc].insert(messages_childs_id.back());
                    ver[main_proc].insert(messages_childs_id.back());
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
                            src = memory.get_perform_data(tid)[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = memory.get_perform_const_data(tid)[d.sourse.id];
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
                    for (process k = 1; k < comm.size(); ++k)
                        ver[k].erase(src);
                    messages_childs_add_id.push_back(memory.add_message_child(d.mes, d.type, src, d.pi));
                    con[main_proc].insert(messages_childs_add_id.back());
                    ver[main_proc].insert(messages_childs_add_id.back());
                    break;
                }
                default:
                    comm.abort(767);
            }
        }

        size_t tid_childs = 0;
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
                    ++tid_childs;
                    break;
                }
                default:
                    comm.abort(767);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    mes_t_id = memory.get_perform_data(tid)[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = memory.get_perform_const_data(tid)[i.mes.id];
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
                        data_id.push_back(memory.get_perform_data(tid)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(memory.get_perform_const_data(tid)[k.id]);
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
                        const_data_id.push_back(memory.get_perform_data(tid)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(memory.get_perform_const_data(tid)[k.id]);
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
                default:
                    comm.abort(767);
            }
        }
        memory.set_perform_created_childs(tid, memory.get_perform_created_childs(tid) + tid_childs);

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
                    parent = tid;
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
                    child = tid;
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
                ready_tasks.push(i.pi);
        }

        for (const task_id& i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                ready_tasks.push(i.pi);
        }

        update_ready_tasks(tid);
    }

    void parallelizer::wait_task(process proc, std::vector<std::set<message_id>>& ver, std::vector<std::set<message_id>>& con, std::vector<std::set<perform_id>>& con_t)
    {
        instruction res_ins;
        instr_comm.recv<message>(&res_ins, proc);

        instruction::const_iterator it = res_ins.begin();
        const instruction_block& ins = *it;

        if (ins.command() != INSTRUCTION::TASK_RES)
            comm.abort(111);
        const instruction_task_result& result = dynamic_cast<const instruction_task_result&>(ins);
        task_id tid = result.id();

        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<std::pair<message_id, message_id>> messages_childs_id;
        std::vector<std::pair<message_id, message_id>> messages_childs_add_id;

        const instruction_block& ins2 = *(++it);
        if (ins2.command() != INSTRUCTION::ADD_RES_TO_MEMORY)
            comm.abort(112);
        const instruction_add_result_to_memory& res_to_mem = dynamic_cast<const instruction_add_result_to_memory&>(ins2);
        messages_init_add_id = res_to_mem.added_messages_init();
        messages_childs_add_id = res_to_mem.added_messages_child();

        const instruction_block& ins3 = *(++it);
        if (ins3.command() != INSTRUCTION::ADD_RES_TO_MEMORY)
            comm.abort(113);
        const instruction_add_result_to_memory& res_to_mem2 = dynamic_cast<const instruction_add_result_to_memory&>(ins3);
        messages_init_id = res_to_mem2.added_messages_init();
        messages_childs_id = res_to_mem2.added_messages_child();

        res_ins.clear();

        request_block res_ins_req;
        task_environment env;
        comm.irecv<message>(&env, proc, res_ins_req);
        res_ins_req.wait_all();

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_task_data(tid))
        {
            for (process k = 0; k < comm.size(); ++k)
                ver[k].erase(i);
            ver[proc].insert(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                j = memory.get_message_parent(j);
                for (process k = 0; k < comm.size(); ++k)
                    ver[k].erase(j);
            }
        }

        for (process k = 0; k < comm.size(); ++k)
            ver[k].erase(tid.mi);
        ver[proc].insert(tid.mi);

        std::vector<message_type> messages_init_id_type;
        std::vector<message_type> messages_init_add_id_type;
        std::vector<message_type> messages_childs_id_type;
        std::vector<message_type> messages_childs_add_id_type;

        for (local_message_id i: env.result_message_ids())
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT_A:
                {
                    messages_init_add_id_type.push_back(env.added_messages_init()[i.id].type);
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    messages_childs_id_type.push_back(env.added_messages_child()[i.id].type);
                    break;
                }
                case MESSAGE_SOURCE::INIT:
                {
                    messages_init_id_type.push_back(env.created_messages_init()[i.id].type);
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    messages_childs_id_type.push_back(env.created_messages_child()[i.id].type);
                    break;
                }
                default:
                    comm.abort(767);
            }
        }

        for (size_t i = 0; i < messages_init_id.size(); ++i)
        {
            con[proc].insert(messages_init_id[i]);
            ver[proc].insert(messages_init_id[i]);
            memory.add_message_to_graph(messages_init_id[i], messages_init_id_type[i]);
        }

        for (size_t i = 0; i < messages_init_add_id.size(); ++i)
        {
            con[proc].insert(messages_init_add_id[i]);
            ver[proc].insert(messages_init_add_id[i]);
            memory.add_message_to_graph(messages_init_add_id[i], messages_init_add_id_type[i]);
        }

        for (size_t i = 0; i < messages_childs_id.size(); ++i)
        {
            con[proc].insert(messages_childs_id[i].second);
            ver[proc].insert(messages_childs_id[i].second);
            memory.add_message_child_to_graph(messages_childs_id[i].second, messages_childs_id_type[i], messages_childs_id[i].first);
            memory.insert_message_child(messages_childs_id[i].first, messages_childs_id[i].second);
        }

        for (size_t i = 0; i < messages_childs_add_id.size(); ++i)
        {
            con[proc].insert(messages_childs_add_id[i].second);
            ver[proc].insert(messages_childs_add_id[i].second);
            memory.add_message_child_to_graph(messages_childs_add_id[i].second, messages_childs_add_id_type[i], messages_childs_add_id[i].first);
            memory.insert_message_child(messages_childs_add_id[i].first, messages_childs_add_id[i].second);
        }

        size_t tid_childs = 0;
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
                    ++tid_childs;
                    break;
                }
                default:
                    comm.abort(767);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    mes_t_id = memory.get_perform_data(tid.pi)[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = memory.get_perform_const_data(tid.pi)[i.mes.id];
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
                    mes_t_id = messages_childs_id[i.mes.id].second;
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    mes_t_id = messages_childs_add_id[i.mes.id].second;
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
                        data_id.push_back(memory.get_perform_data(tid.pi)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(memory.get_perform_const_data(tid.pi)[k.id]);
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
                        data_id.push_back(messages_childs_id[k.id].second);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        data_id.push_back(messages_childs_add_id[k.id].second);
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
                        const_data_id.push_back(memory.get_perform_data(tid.pi)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(memory.get_perform_const_data(tid.pi)[k.id]);
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
                        const_data_id.push_back(messages_childs_id[k.id].second);
                        break;
                    }
                    case MESSAGE_SOURCE::CHILD_A:
                    {
                        const_data_id.push_back(messages_childs_add_id[k.id].second);
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
                    comm.abort(767);
            }
        }
        memory.set_task_created_childs(tid, memory.get_task_created_childs(tid) + tid_childs);

        for (const task_dependence& i: env.created_dependences())
        {
            task_id parent {};
            task_id child {};

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
                    parent = tid;
                    break;
                }
                default:
                    comm.abort(767);
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
                    child = tid;
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
                ready_tasks.push(i.pi);
        }

        for (const task_id& i: tasks_child_id)
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
            {
                for (perform_id i: memory.get_perform_childs(c_t))
                {
                    memory.set_perform_parents_count(i, memory.get_perform_parents_count(i) - 1);
                    if (memory.get_perform_parents_count(i) == 0)
                        ready_tasks.push(i);
                }
                tasks_to_del.push_back(c_t);
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
        std::vector<std::unique_ptr<std::thread>> task_execution_thread_v(execution_thread_count - 1);
        for (auto& i : task_execution_thread_v)
            i.reset(new std::thread(&parallelizer::task_execution_thread_function, this, comm.size()));

        instruction cur_inst;
        while(1)
        {
            bool queue_try = false;
            bool comm_try = false;

            if (instr_comm.test_process(main_proc))
            {
                instr_comm.recv<message>(&cur_inst, main_proc);

                for (const instruction_block& i: cur_inst)
                {
                    switch (i.command())
                    {
                    case INSTRUCTION::MES_SEND:
                    {
                        const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                        comm.isend(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
                        break;
                    }
                    case INSTRUCTION::MES_RECV:
                    {
                        const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                        comm.irecv(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
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
                        std::vector<message*> iib = message_init_factory::get_info(j.type());
                        request_block info_req;
                        for (message* p: iib)
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
                        for (message* p: pib)
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
                    case INSTRUCTION::TASK_CREATE:
                    {
                        const instruction_task_create& j = dynamic_cast<const instruction_task_create&>(i);
                        memory.add_perform_with_id(j.id(), j.type(), j.data(), j.const_data());
                        break;
                    }
                    case INSTRUCTION::TASK_EXE:
                    {
                        const instruction_task_execute& j = dynamic_cast<const instruction_task_execute&>(i);
                        execute_task(j.id().pi);
                        break;
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
                    case INSTRUCTION::END:

                        goto end;

                    default:

                        instr_comm.abort(234);
                    }
                }
                cur_inst.clear();
                comm_try = true;
            }

            queue_try = finished_task_queue_mutex.try_lock();
            if (queue_try)
            {
                if (finished_task_queue.empty())
                {
                    queue_try = false;
                    finished_task_queue_mutex.unlock();
                }
                else
                {
                    finished_task_execution_queue_data current_finished_task_data{ finished_task_queue.front() };
                    finished_task_queue.pop();
                    finished_task_queue_mutex.unlock();

                    worker_task_finishing(current_finished_task_data);
                }
            }

            if (!queue_try && !comm_try)
            {
                task_queue_mutex.lock();
                if (task_queue.empty())
                {
                    task_queue_mutex.unlock();
                    std::this_thread::yield();
                    continue;
                }

                task_execution_queue_data current_execution_data{ task_queue.front() };
                task_queue.pop();
                task_queue_mutex.unlock();

                finished_task_execution_queue_data current_output_data{ current_execution_data.this_task_id, {current_execution_data.task_type,
                    current_execution_data.args.size(), current_execution_data.const_args.size(), static_cast<size_t>(comm.size()), execution_thread_count} };
                current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
                task_factory::perform(current_execution_data.task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
                current_execution_data.this_task->set_environment(nullptr);

                finished_task_queue_mutex.lock();
                finished_task_queue.push(std::move(current_output_data));
                finished_task_queue_mutex.unlock();
            }
        }
        end:
        task_execution_queue_data finish_queue_data;
        finish_queue_data.this_task = nullptr;
        task_queue_mutex.lock();
        task_queue.push(finish_queue_data);
        task_queue_mutex.unlock();
        for (auto& i: task_execution_thread_v)
            i->join();
    }

    void parallelizer::worker_task_finishing(finished_task_execution_queue_data& cur_task_exe_data)
    {
        std::vector<message_id> added_m_init, messages_init_id;
        std::vector<std::pair<message_id, message_id>> added_m_child, messages_childs_id;
        for (const local_message_id& i : cur_task_exe_data.this_task_environment.result_message_ids())
        {
            switch (i.src)
            {
            case MESSAGE_SOURCE::INIT_A:
            {
                message_init_add_data& d = cur_task_exe_data.this_task_environment.added_messages_init()[i.id];
                added_m_init.push_back(memory.add_message_init(d.mes, d.type, d.ii));
                break;
            }
            case MESSAGE_SOURCE::CHILD_A:
            {
                message_child_add_data& d = cur_task_exe_data.this_task_environment.added_messages_child()[i.id];

                message_id src = MESSAGE_ID_UNDEFINED;
                switch (d.sourse.src)
                {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    src = memory.get_perform_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    src = memory.get_perform_const_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT:
                {
                    src = messages_init_id[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    src = added_m_init[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    src = messages_childs_id[d.sourse.id].second;
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    src = added_m_child[d.sourse.id].second;
                    break;
                }
                default:
                    comm.abort(767);
                }
                added_m_child.push_back({ src, memory.add_message_child(d.mes, d.type, src, d.pi) });
                break;
            }
            case MESSAGE_SOURCE::INIT:
            {
                message_init_data& d = cur_task_exe_data.this_task_environment.created_messages_init()[i.id];
                messages_init_id.push_back(memory.create_message_init(d.type, d.ii));
                break;
            }
            case MESSAGE_SOURCE::CHILD:
            {
                message_child_data& d = cur_task_exe_data.this_task_environment.created_messages_child()[i.id];

                message_id src = MESSAGE_ID_UNDEFINED;
                switch (d.sourse.src)
                {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    src = memory.get_perform_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    src = memory.get_perform_const_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT:
                {
                    src = messages_init_id[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    src = added_m_init[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::CHILD:
                {
                    src = messages_childs_id[d.sourse.id].second;
                    break;
                }
                case MESSAGE_SOURCE::CHILD_A:
                {
                    src = added_m_child[d.sourse.id].second;
                    break;
                }
                default:
                    comm.abort(767);
                }
                messages_childs_id.push_back({ src, memory.create_message_child(d.type, src, d.pi) });
                break;
            }
            default:
                comm.abort(767);
            }
        }

        instruction res;
        res.add_task_result(memory.get_task_id(cur_task_exe_data.this_task_id));
        res.add_add_result_to_memory(added_m_init, added_m_child);
        res.add_add_result_to_memory(messages_init_id, messages_childs_id);

        request_block req;
        instr_comm.send<message>(&res, main_proc);
        comm.isend<message>(&cur_task_exe_data.this_task_environment, main_proc, req);
        req.wait_all();
    }

    void parallelizer::execute_task(perform_id id)
    {
        task_execution_queue_data current_data{ id, memory.get_task(memory.get_task_id(id).mi), memory.get_perform_type(id) };
        for (message_id j: memory.get_perform_data(id))
            current_data.args.push_back(memory.get_message(j));
        for (message_id j: memory.get_perform_const_data(id))
            current_data.const_args.push_back(memory.get_message(j));

        task_queue_mutex.lock();
        task_queue.push(std::move(current_data));
        task_queue_mutex.unlock();
    }

    process parallelizer::get_current_proc()
    { return comm.rank(); }

    size_t parallelizer::get_workers_count()
    { return comm.size() * execution_thread_count; }

    void parallelizer::clear()
    { memory.clear(); }

}
