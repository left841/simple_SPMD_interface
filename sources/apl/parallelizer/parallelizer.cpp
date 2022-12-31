#include "apl/parallelizer/parallelizer.h"

namespace apl
{

    void parallelizer::master_exe_info::send(const sender& se) const
    {
        se.send(process_workgroup.data(), process_workgroup.size());
        std::vector<message_id> tmp_vec;
        for (const auto& i: contained_messages)
        {
            for (auto it = i.cbegin(); it != i.cend(); ++it)
                tmp_vec.push_back(*it);
            se.send(tmp_vec.data(), tmp_vec.size());
            tmp_vec.clear();
        }
        for (const auto& i: versions_of_messages)
        {
            for (auto it = i.cbegin(); it != i.cend(); ++it)
                tmp_vec.push_back(*it);
            se.send(tmp_vec.data(), tmp_vec.size());
            tmp_vec.clear();
        }
    }

    void parallelizer::master_exe_info::recv(const receiver& re)
    {
        process_workgroup.resize(re.probe<process>());
        re.recv(process_workgroup.data(), process_workgroup.size());
        std::vector<message_id> tmp_vec;
        contained_messages.resize(process_workgroup.size());
        for (auto& i: contained_messages)
        {
            tmp_vec.resize(re.probe<message_id>());
            re.recv(tmp_vec.data(), tmp_vec.size());
            i.clear();
            for (auto j: tmp_vec)
                i.insert(j);
            tmp_vec.clear();
        }
        versions_of_messages.resize(process_workgroup.size());
        for (auto& i: versions_of_messages)
        {
            tmp_vec.resize(re.probe<message_id>());
            re.recv(tmp_vec.data(), tmp_vec.size());
            i.clear();
            for (auto j: tmp_vec)
                i.insert(j);
            tmp_vec.clear();
        }
        process_positions.clear();
        for (size_t i = 0; i < process_workgroup.size(); ++i)
            process_positions[process_workgroup[i]] = i;
        instructions_for_processes.resize(process_workgroup.size());
    }

    parallelizer::parallelizer(size_t thread_count, const intracomm& _comm): main_comm(_comm), instr_comm(main_comm), execution_thread_count(thread_count)
    { }

    parallelizer::parallelizer(task_graph& _tg, size_t thread_count, const intracomm& _comm): main_comm(_comm), instr_comm(main_comm), memory(_tg), execution_thread_count(thread_count)
    { }

    parallelizer::~parallelizer()
    { }

    void parallelizer::init(task_graph& _tg)
    { memory.init(_tg); }

    void parallelizer::execution()
    {
        std::vector<std::unique_ptr<std::thread>> task_execution_thread_v(execution_thread_count - 1);
        for (auto& i: task_execution_thread_v)
            i.reset(new std::thread(&parallelizer::task_execution_thread_function, this, main_comm.size()));

        if (main_comm.rank() == main_proc)
        {
            master_exe_info main_info;
            main_info.process_workgroup.resize(main_comm.size());
            for (size_t i = 0; i < main_info.process_workgroup.size(); ++i)
            {
                main_info.process_workgroup[i] = i;
                main_info.process_positions[i] = i;
            }

            main_info.versions_of_messages.resize(main_comm.size());
            main_info.versions_of_messages[0] = memory.get_messages_set();
            for (process i = 1; i < main_comm.size(); ++i)
                main_info.versions_of_messages[i] = main_info.versions_of_messages[0];

            main_info.contained_messages.resize(main_comm.size());
            for (process i = 0; i < main_comm.size(); ++i)
                main_info.contained_messages[i] = main_info.versions_of_messages[0];

            main_info.instructions_for_processes.resize(main_comm.size());
            main_info.ready_tasks = std::move(memory.get_ready_tasks());

            master(main_info);

            instruction end;
            end.add_end();
            for (size_t i = 1; i < main_info.process_workgroup.size(); ++i)
                instr_comm.send<message>(&end, main_info.process_workgroup[i]);
        }
        else
        {
            std::set<task_id> tmp = memory.get_tasks_set();
            for (auto it = tmp.begin(); it != tmp.end(); ++it)
                memory.delete_task(*it);
            worker(main_comm.size());
        }

        {
            std::unique_lock<std::mutex> cv_ul(task_queue_mutex);
            task_execution_queue_data exe_thread_end_data;
            exe_thread_end_data.this_task = nullptr;
            task_queue.push(exe_thread_end_data);
            exe_threads_cv.notify_one();
        }
        for (auto& i: task_execution_thread_v)
            i->join();
        clear();
    }

    void parallelizer::execution(task_graph& _tg)
    {
        init(_tg);
        execution();
    }

    void parallelizer::master(master_exe_info& info)
    {
        std::vector<std::vector<task_id>> assigned_tasks(info.process_workgroup.size());
        size_t all_assigned_tasks_count = 0;

        std::vector<task_id> tasks_to_del;

        struct waiting_exeternal_task
        {
            process proc;
            task_id task;
        };

        std::map<process, task_id> external_tasks;

        bool exe = true;

        while (exe)
        {
            //std::cout << comm_world.rank() << ": " << "master start_cycle" << std::endl;
            /*for (task_id i: tasks_to_del)
                memory.delete_task(i);
            tasks_to_del.clear();
            while (memory.get_unreferenced_messages().size())
            {
                message_id i = *memory.get_unreferenced_messages().begin();
                std::cout << comm_world.rank() << ": " << "deleting message: " << i.num << '_' << i.proc << std::endl;
                memory.delete_message_from_graph(i);

                for (size_t j = 0; j < info.process_workgroup.size(); ++j)
                {
                    if (info.contained_messages[j].find(i) != info.contained_messages[j].end())
                    {
                        info.contained_messages[j].erase(i);
                        info.versions_of_messages[j].erase(i);
                        info.instructions_for_processes[j].add_message_del(i);
                    }
                }
            }*/
            //std::cout << comm_world.rank() << ": " << "delete_tasks" << std::endl;
            {
                std::vector<size_t> comm_workload(info.process_workgroup.size());

                std::vector<task_id> ready_task_v;
                std::vector<task_with_process_count> ready_task_with_proc_count_v;

                std::vector<size_t> current_workgroup(info.process_workgroup.size());
                for (size_t i = 0; i < current_workgroup.size(); ++i)
                    current_workgroup[i] = i;

                while (info.ready_tasks.size())
                {
                    if (info.ready_tasks.front().preferred_processes_count == 0)
                        ready_task_v.push_back(info.ready_tasks.front().this_task);
                    else
                        ready_task_with_proc_count_v.push_back(info.ready_tasks.front());
                    info.ready_tasks.pop();
                }
                //std::cout << comm_world.rank() << ": " << "info.ready_tasks emptyed" << std::endl;

                //if (ready_task_with_proc_count_v.size() > 0)
                    //main_comm.abort(651);
                struct task_with_group
                {
                    task_id this_task;
                    std::vector<size_t> group;
                };
                std::vector<task_with_group> task_with_group_v;

                for (task_with_process_count& current_task: ready_task_with_proc_count_v)
                {
                    if (current_task.preferred_processes_count >= current_workgroup.size())
                    {
                        ready_task_v.push_back(current_task.this_task);
                        continue;
                    }

                    std::vector<size_t> decision_vector;
                    std::vector<double> contained_task_data_counts(current_workgroup.size(), 0.0);
                    std::vector<message_id>& cur_task_data_ids = memory.get_task_data(current_task.this_task);
                    std::vector<message_id>& cur_task_const_data_ids = memory.get_task_const_data(current_task.this_task);

                    for (size_t i = 1; i < current_workgroup.size(); ++i)
                    {
                        decision_vector.push_back(i);
                        for (auto j: cur_task_data_ids)
                        {
                            size_t depth = 0;
                            message_id current = j;
                            while ((current != MESSAGE_ID_UNDEFINED) && (depth < current_workgroup.size()))
                            {
                                if (info.versions_of_messages[current_workgroup[i]].find(current) != info.versions_of_messages[current_workgroup[i]].end())
                                {
                                    contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(current_workgroup.size());
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
                                if (info.versions_of_messages[current_workgroup[i]].find(current) != info.versions_of_messages[current_workgroup[i]].end())
                                {
                                    contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(current_workgroup.size());
                                    break;
                                }
                                ++depth;
                                current = memory.get_message_parent(current);
                            }
                        }
                    }
                    //std::cout << comm_world.rank() << ": " << "checked_task_data_contained: " << current_task.this_task.num << '_' << current_task.this_task.proc << std::endl;
                    std::sort(decision_vector.begin(), decision_vector.end(), [&](size_t a, size_t b)->bool
                    {
                        if (contained_task_data_counts[a] != contained_task_data_counts[b])
                            return contained_task_data_counts[a] > contained_task_data_counts[b];
                        return a > b;
                    });
                    //std::cout << comm_world.rank() << ": " << "sorted_processes_for_task: " << current_task.this_task.num << '_' << current_task.this_task.proc << std::endl;
                    std::vector<size_t> new_task_group;
                    std::set<size_t> new_group;
                    //std::cout << comm_world.rank() << ": " << "adding_prefered_processes: " << current_task.preferred_processes_count << " to size: " << decision_vector.size() << std::endl;
                    
                    //std::cout << comm_world.rank() << ": " << "decesion_vector: ";
                    //for (auto& i: decision_vector)
                    //    std::cout << i << ' ';
                    //std::cout << std::endl;
                    //std::cout << comm_world.rank() << ": " << "current_workgroup: ";
                    //for (auto& i: current_workgroup)
                    //    std::cout << i << ' ';
                    //std::cout << std::endl;

                    for (size_t i = 0; (i < decision_vector.size()) && (i < current_task.preferred_processes_count); ++i)
                    {
                        new_task_group.push_back(current_workgroup[decision_vector[i]]);
                        new_group.insert(current_workgroup[decision_vector[i]]);
                    }
                    //std::cout << comm_world.rank() << ": " << "added_prefered_processes" << std::endl;
                    //std::cout << comm_world.rank() << ": " << "sending_task_data to group: " << current_workgroup[decision_vector[0]] << std::endl;
                    send_task_data(current_task.this_task, current_workgroup[decision_vector[0]], comm_workload, info);
                    //std::cout << comm_world.rank() << ": " << "sended_task_data: " << current_task.this_task.num << '_' << current_task.this_task.proc << std::endl;

                    std::vector<size_t> new_current_workgroup;
                    for (auto i: current_workgroup)
                    {
                        if (new_group.find(i) == new_group.end())
                            new_current_workgroup.push_back(i);
                    }
                    std::swap(current_workgroup, new_current_workgroup);

                    task_with_group_v.push_back({current_task.this_task, new_task_group});
                }
                //std::cout << comm_world.rank() << ": " << "lol" << std::endl;
                size_t comm_size_quotient = ready_task_v.size() / current_workgroup.size();
                size_t comm_size_remainder = ready_task_v.size() % current_workgroup.size();
                assigned_tasks.resize(current_workgroup.size());
                for (size_t i = 0; i < current_workgroup.size(); ++i)
                {
                    size_t max_assigned_tasks_count = comm_size_quotient + ((i < comm_size_remainder) ? 1 : 0);
                    assigned_tasks[i].resize(max_assigned_tasks_count);
                    assigned_tasks[i].assign(max_assigned_tasks_count, TASK_ID_UNDEFINED);
                }
                //std::cout << comm_world.rank() << ": " << "resized_assigned_tasks: " << assigned_tasks.size() << std::endl;
                std::vector<size_t> assigned_current_position(current_workgroup.size(), 0);
                for (task_id current_id: ready_task_v)
                {
                    //std::cout << comm_world.rank() << ": " << "processing_task_id: " << current_id.num << '_' << current_id.proc << std::endl;

                    std::vector<size_t> decision_vector;
                    std::vector<double> contained_task_data_counts(current_workgroup.size(), 0.0);
                    std::vector<message_id>& cur_task_data_ids = memory.get_task_data(current_id);
                    std::vector<message_id>& cur_task_const_data_ids = memory.get_task_const_data(current_id);

                    //std::cout << comm_world.rank() << ": " << "current_workgroup: ";
                    //for (auto& i: current_workgroup)
                    //    std::cout << i << ' ';
                    //std::cout << std::endl;

                    for (size_t i = 0; i < current_workgroup.size(); ++i)
                    {
                        //std::cout << comm_world.rank() << ": " << "assigned_position: " << assigned_current_position.size() << ' ' << assigned_tasks.size() << std::endl;

                        if (assigned_current_position[i] < assigned_tasks[i].size())
                        {
                            decision_vector.push_back(i);
                            for (auto j: cur_task_data_ids)
                            {
                                size_t depth = 0;
                                message_id current = j;
                                //std::cout << comm_world.rank() << ": " << "current_message: " << current.num << '_' << current.proc << std::endl;
                                while ((current != MESSAGE_ID_UNDEFINED) && (depth < current_workgroup.size()))
                                {
                                    //std::cout << comm_world.rank() << ": " << "current_message: " << current.num << '_' << current.proc << std::endl;
                                    //std::cout << comm_world.rank() << ": " << "versions_of_messages_size: " << info.versions_of_messages.size() << std::endl;

                                    if (info.versions_of_messages[current_workgroup[i]].find(current) != info.versions_of_messages[current_workgroup[i]].end())
                                    {
                                        //std::cout << comm_world.rank() << ": " << "current_message_found: " << current.num << '_' << current.proc << std::endl;
                                        contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(current_workgroup.size());
                                        break;
                                    }
                                    ++depth;
                                    //std::cout << comm_world.rank() << ": " << "finding_parent_for: " << current.num << '_' << current.proc << std::endl;
                                    current = memory.get_message_parent(current);
                                }
                            }
                            for (auto j: cur_task_const_data_ids)
                            {
                                size_t depth = 0;
                                message_id current = j;
                                while (current != MESSAGE_ID_UNDEFINED)
                                {
                                    if (info.versions_of_messages[current_workgroup[i]].find(current) != info.versions_of_messages[current_workgroup[i]].end())
                                    {
                                        contained_task_data_counts[i] += 1.0 - static_cast<double>(depth) / static_cast<double>(current_workgroup.size());
                                        break;
                                    }
                                    ++depth;
                                    current = memory.get_message_parent(current);
                                }
                            }
                        }
                    }
                    //std::cout << comm_world.rank() << ": " << "sorting_decesion_vector: " << std::endl;
                    std::sort(decision_vector.begin(), decision_vector.end(), [&](size_t a, size_t b)->bool
                    {
                        if (contained_task_data_counts[a] != contained_task_data_counts[b])
                            return contained_task_data_counts[a] > contained_task_data_counts[b];
                        return a > b;
                    });

                    //std::cout << comm_world.rank() << ": " << "decesion_vector: ";
                    //for (auto& i : decision_vector)
                    //    std::cout << i << ' ';
                    //std::cout << std::endl;

                    size_t current_proc = decision_vector[0];
                    assigned_tasks[current_proc][assigned_current_position[current_proc]++] = current_id;
                    send_task_data(current_id, current_workgroup[current_proc], comm_workload, info);
                    ++all_assigned_tasks_count;
                }
                comm_workload.clear();
                //std::cout << comm_world.rank() << ": " << "lal" << std::endl;
                for (size_t i = 1; i < current_workgroup.size(); ++i)
                    for (task_id j: assigned_tasks[i])
                    {
                        info.instructions_for_processes[current_workgroup[i]].add_task_creation(j, memory.get_task_base_message(j), memory.get_task_type(j), memory.get_task_data(j), memory.get_task_const_data(j));
                        info.instructions_for_processes[current_workgroup[i]].add_task_execution(j);
                    }
                //std::cout << comm_world.rank() << ": " << "added_task_creation_for_all" << std::endl;
                for (size_t i = 1; i < info.instructions_for_processes.size(); ++i)
                {
                    if (info.instructions_for_processes[i].size() > 0)
                    {
                        instr_comm.send<message>(&info.instructions_for_processes[i], info.process_workgroup[i]);
                        info.instructions_for_processes[i].clear();
                    }
                }
                send_instruction(info.instructions_for_processes[0]);
                info.instructions_for_processes[0].clear();
                //std::cout << comm_world.rank() << ": " << "sended_instructions" << std::endl;

                std::set<size_t> groups_to_del;
                for (auto i: task_with_group_v)
                {
                    instruction cur_ins;
                    cur_ins.add_task_creation(i.this_task, memory.get_task_base_message(i.this_task), memory.get_task_type(i.this_task), memory.get_task_data(i.this_task), memory.get_task_const_data(i.this_task));
                    cur_ins.add_independent_exe(i.this_task);
                    instr_comm.send<message>(&cur_ins, info.process_workgroup[i.group[0]]);
                    cur_ins.clear();

                    master_exe_info this_group_exe_info;
                    for (size_t j = 0; j < i.group.size(); ++j)
                    {
                        this_group_exe_info.process_workgroup.push_back(info.process_workgroup[i.group[j]]);
                        this_group_exe_info.contained_messages.push_back(info.contained_messages[i.group[j]]);
                        this_group_exe_info.versions_of_messages.push_back(info.versions_of_messages[i.group[j]]);
                        groups_to_del.insert(i.group[j]);
                    }
                    main_comm.send<message>(&this_group_exe_info, info.process_workgroup[i.group[0]]);

                    for (size_t j = 1; j < i.group.size(); ++j)
                    {
                        cur_ins.add_change_owner(info.process_workgroup[i.group[0]], i.group.size());
                        instr_comm.send<message>(&cur_ins, info.process_workgroup[i.group[j]]);
                        cur_ins.clear();
                    }

                    external_tasks[info.process_workgroup[i.group[0]]] = i.this_task;
                }

                master_exe_info new_exe_info;
                for (size_t i = 0; i < info.process_workgroup.size(); ++i)
                {
                    if (groups_to_del.find(i) == groups_to_del.end())
                    {
                        instruction ins;
                        new_exe_info.instructions_for_processes.push_back(ins);
                        new_exe_info.process_workgroup.push_back(info.process_workgroup[i]);
                        new_exe_info.contained_messages.push_back(info.contained_messages[i]);
                        new_exe_info.versions_of_messages.push_back(info.versions_of_messages[i]);
                    }
                }
                for (size_t i = 0; i < new_exe_info.process_workgroup.size(); ++i)
                    new_exe_info.process_positions[new_exe_info.process_workgroup[i]] = i;
                std::swap(new_exe_info, info);
                //std::cout << comm_world.rank() << ": " << "lil" << std::endl;
            }

            for (task_id i: assigned_tasks[0])
            {
                task_execution_queue_data current_data {i, memory.get_message_as_task(memory.get_task_base_message(i)), memory.get_task_type(i)};
                for (message_id j: memory.get_task_data(i))
                    current_data.args.push_back(memory.get_message(j));
                for (message_id j: memory.get_task_const_data(i))
                    current_data.const_args.push_back(memory.get_message(j));

                task_queue_mutex.lock();
                task_queue.push(std::move(current_data));
                task_queue_mutex.unlock();
            }
            {
                std::unique_lock<std::mutex> cv_lk(task_queue_mutex);
                exe_threads_cv.notify_all();
            }
            //std::cout << comm_world.rank() << ": " << "lool" << std::endl;
            while ((all_assigned_tasks_count > 0) && (assigned_tasks[0].size() > 0))
            {
                //std::cout << comm_world.rank() << ": " << "liil" << std::endl;
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
                        //std::cout << comm_world.rank() << ": " << "loool" << std::endl;
                        end_main_task(0, current_finished_task_data.this_task_id, current_finished_task_data.this_task_environment, tasks_to_del, info);
                        //std::cout << comm_world.rank() << ": " << "laaal" << std::endl;
                        assigned_tasks[0].pop_back();
                        --all_assigned_tasks_count;
                    }
                }
                //std::cout << assigned_tasks[0].size() << " luul" << std::endl;
                process current_proc = instr_comm.test_any_process();
                if (current_proc != MPI_PROC_NULL)
                {
                    //std::cout << comm_world.rank() << ": " << "found_proc: " << current_proc << std::endl;
                    //std::cout << comm_world.rank() << ": " << "external_task_size: ";
                    //for (auto& i: external_tasks)
                    //    std::cout << i.first << '=' << i.second.num << '_' << i.second.proc << ' ' << std::endl;
                    //std::cout << std::endl;

                    if (external_tasks.find(current_proc) != external_tasks.end())
                    {
                        //std::cout << comm_world.rank() << ": " << "wut: " << current_proc << std::endl;
                        process current_process = current_proc;
                        {
                            instruction ins;
                            instr_comm.recv<message>(&ins, current_process);

                            for (const instruction_block& i: ins)
                            {
                                switch (i.command())
                                {
                                case INSTRUCTION::PACKED_MESSAGE_GRAPH:
                                {
                                    const instruction_packed_message_graph& j = dynamic_cast<const instruction_packed_message_graph&>(i);
                                    if (!memory.message_contained(j.id()));
                                    {
                                        if (j.f_type() == MESSAGE_FACTORY_TYPE::INIT)
                                            memory.add_message_to_graph(j.id(), j.type());
                                        else
                                            memory.add_message_child_to_graph(j.id(), j.type(), j.parent());
                                    }
                                    memory.set_message_child_state(j.id(), j.ch_state());
                                    std::set<message_id> s(std::move(j.childs()));
                                    for (auto k: s)
                                        memory.insert_message_child(j.id(), k);

                                    break;
                                }
                                default:
                                    main_comm.abort(576);
                                }
                            }
                        }
                        master_exe_info received_exe_info;
                        main_comm.recv<message>(&received_exe_info, current_process);

                        task_id current_id = external_tasks.find(current_process)->second;
                        external_tasks.erase(current_process);


                        for (message_id i: memory.get_task_data(current_id))
                        {
                            for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                                info.versions_of_messages[k].erase(i);
                            message_id j = i;
                            while (memory.message_has_parent(j))
                            {
                                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                                j = memory.get_message_parent(j);
                                for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                                    info.versions_of_messages[k].erase(j);
                            }
                        }

                        message_id tid_base = memory.get_task_base_message(current_id);
                        for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                            info.versions_of_messages[k].erase(tid_base);


                        for (size_t i = 0; i < received_exe_info.process_workgroup.size(); ++i)
                        {
                            info.process_workgroup.push_back(received_exe_info.process_workgroup[i]);
                            instruction ins;
                            info.instructions_for_processes.push_back(ins);
                            info.contained_messages.push_back(received_exe_info.contained_messages[i]);
                            info.versions_of_messages.push_back(received_exe_info.versions_of_messages[i]);
                            info.process_positions[received_exe_info.process_workgroup[i]] = info.process_workgroup.size() - 1;
                        }


                        update_ready_tasks(current_id, tasks_to_del, info);
                    }
                    else
                    {
                        comm_try = true;
                        wait_task(info.process_positions[current_proc], tasks_to_del, info);
                        --all_assigned_tasks_count;
                    }
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

                    finished_task_execution_queue_data current_output_data{ current_execution_data.this_task_id, {current_execution_data.this_task_type,
                        current_execution_data.args.size(), current_execution_data.const_args.size(), info.process_workgroup.size(), execution_thread_count} };
                    current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
                    task_factory::perform(current_execution_data.this_task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
                    current_execution_data.this_task->set_environment(nullptr);

                    finished_task_queue_mutex.lock();
                    finished_task_queue.push(std::move(current_output_data));
                    finished_task_queue_mutex.unlock();
                }
                //std::cout << comm_world.rank() << ": " << "laal" << std::endl;
            }
            //std::cout << comm_world.rank() << ": " << "lel" << std::endl;
            while (all_assigned_tasks_count > 0)
            {
                process current_proc = instr_comm.wait_any_process();
                //std::cout << comm_world.rank() << ": " << "waited_for_process: " << current_proc << std::endl;
                if (external_tasks.find(current_proc) != external_tasks.end())
                {
                    process current_process = current_proc;
                    {
                        instruction ins;
                        instr_comm.recv<message>(&ins, current_process);

                        for (const instruction_block& i: ins)
                        {
                            switch (i.command())
                            {
                            case INSTRUCTION::PACKED_MESSAGE_GRAPH:
                            {
                                const instruction_packed_message_graph& j = dynamic_cast<const instruction_packed_message_graph&>(i);
                                if (!memory.message_contained(j.id()));
                                {
                                    if (j.f_type() == MESSAGE_FACTORY_TYPE::INIT)
                                        memory.add_message_to_graph(j.id(), j.type());
                                    else
                                        memory.add_message_child_to_graph(j.id(), j.type(), j.parent());
                                }
                                memory.set_message_child_state(j.id(), j.ch_state());
                                std::set<message_id> s(std::move(j.childs()));
                                for (auto k: s)
                                    memory.insert_message_child(j.id(), k);
                                
                                break;
                            }
                            default:
                                main_comm.abort(576);
                            }
                        }
                    }
                    master_exe_info received_exe_info;
                    main_comm.recv<message>(&received_exe_info, current_process);

                    task_id current_id = external_tasks.find(current_process)->second;
                    external_tasks.erase(current_process);


                    for (message_id i: memory.get_task_data(current_id))
                    {
                        for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                            info.versions_of_messages[k].erase(i);
                        message_id j = i;
                        while (memory.message_has_parent(j))
                        {
                            if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                                memory.set_message_child_state(j, CHILD_STATE::NEWER);
                            j = memory.get_message_parent(j);
                            for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                                info.versions_of_messages[k].erase(j);
                        }
                    }

                    message_id tid_base = memory.get_task_base_message(current_id);
                    for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                        info.versions_of_messages[k].erase(tid_base);


                    for (size_t i = 0; i < received_exe_info.process_workgroup.size(); ++i)
                    {
                        info.process_workgroup.push_back(received_exe_info.process_workgroup[i]);
                        instruction ins;
                        info.instructions_for_processes.push_back(ins);
                        info.contained_messages.push_back(received_exe_info.contained_messages[i]);
                        info.versions_of_messages.push_back(received_exe_info.versions_of_messages[i]);
                        info.process_positions[received_exe_info.process_workgroup[i]] = info.process_workgroup.size() - 1;
                    }

                    update_ready_tasks(current_id, tasks_to_del, info);
                }
                else
                {
                    wait_task(info.process_positions[current_proc], tasks_to_del, info);
                    --all_assigned_tasks_count;
                }
            }

            for (auto& i: assigned_tasks)
            {
                i.clear();
            }
            //std::cout << comm_world.rank() << ": " << "leeel" << std::endl;
            // waiting for external tasks
            if (info.ready_tasks.empty() && !external_tasks.empty())
            {
                //std::cout << comm_world.rank() << ": " << "start_wait2_for_process ext_task_size: " << external_tasks.size() << std::endl;
                process current_process = instr_comm.wait_any_process();
                //std::cout << comm_world.rank() << ": " << "waited2_for_process: " << current_process << std::endl;
                {
                    instruction ins;
                    instr_comm.recv<message>(&ins, current_process);

                    for (const instruction_block& i : ins)
                    {
                        switch (i.command())
                        {
                        case INSTRUCTION::PACKED_MESSAGE_GRAPH:
                        {
                            const instruction_packed_message_graph& j = dynamic_cast<const instruction_packed_message_graph&>(i);
                            if (!memory.message_contained(j.id()));
                            {
                                if (j.f_type() == MESSAGE_FACTORY_TYPE::INIT)
                                    memory.add_message_to_graph(j.id(), j.type());
                                else
                                    memory.add_message_child_to_graph(j.id(), j.type(), j.parent());
                            }
                            memory.set_message_child_state(j.id(), j.ch_state());
                            std::set<message_id> s(std::move(j.childs()));
                            for (auto k: s)
                                memory.insert_message_child(j.id(), k);

                            break;
                        }
                        default:
                            main_comm.abort(576);
                        }
                    }
                }
                master_exe_info received_exe_info;
                main_comm.recv<message>(&received_exe_info, current_process);

                task_id current_id = external_tasks.find(current_process)->second;
                external_tasks.erase(current_process);


                for (message_id i: memory.get_task_data(current_id))
                {
                    for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                        info.versions_of_messages[k].erase(i);
                    message_id j = i;
                    while (memory.message_has_parent(j))
                    {
                        if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                            memory.set_message_child_state(j, CHILD_STATE::NEWER);
                        j = memory.get_message_parent(j);
                        for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                            info.versions_of_messages[k].erase(j);
                    }
                }

                message_id tid_base = memory.get_task_base_message(current_id);
                for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                    info.versions_of_messages[k].erase(tid_base);


                for (size_t i = 0; i < received_exe_info.process_workgroup.size(); ++i)
                {
                    info.process_workgroup.push_back(received_exe_info.process_workgroup[i]);
                    instruction ins;
                    info.instructions_for_processes.push_back(ins);
                    info.contained_messages.push_back(received_exe_info.contained_messages[i]);
                    info.versions_of_messages.push_back(received_exe_info.versions_of_messages[i]);
                    info.process_positions[received_exe_info.process_workgroup[i]] = info.process_workgroup.size() - 1;
                }

                update_ready_tasks(current_id, tasks_to_del, info);
            }

            if (info.ready_tasks.empty() && external_tasks.empty())
                exe = false;
        }
        //std::cout << comm_world.rank() << ": " << "ended master: " << main_comm.rank() << std::endl;
    }

    void parallelizer::task_execution_thread_function(size_t processes_count)
    {
        bool execution_continued = true;
        while (execution_continued)
        {
            {
                std::unique_lock<std::mutex> cv_ul(task_queue_mutex);
                exe_threads_cv.wait(cv_ul, [&]()
                {
                    return !task_queue.empty();
                });
            }

            while (true)
            {
                task_queue_mutex.lock();
                if (task_queue.empty())
                {
                    task_queue_mutex.unlock();
                    break;
                }
                task_execution_queue_data current_execution_data{ task_queue.front() };
                task_queue.pop();
                task_queue_mutex.unlock();

                if (current_execution_data.this_task == nullptr)
                {
                    execution_continued = false;
                    break;
                }

                finished_task_execution_queue_data current_output_data{ current_execution_data.this_task_id, {current_execution_data.this_task_type,
                    current_execution_data.args.size(), current_execution_data.const_args.size(), processes_count, execution_thread_count} };
                current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
                task_factory::perform(current_execution_data.this_task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
                current_execution_data.this_task->set_environment(nullptr);

                finished_task_queue_mutex.lock();
                finished_task_queue.push(std::move(current_output_data));
                finished_task_queue_mutex.unlock();
            }
        }

        {
            std::unique_lock<std::mutex> cv_ul(task_queue_mutex);
            task_execution_queue_data exe_thread_end_data;
            exe_thread_end_data.this_task = nullptr;
            task_queue.push(exe_thread_end_data);
            exe_threads_cv.notify_one();
        }
    }

    void parallelizer::send_task_data(task_id tid, size_t proc, std::vector<size_t>& comm_workload, master_exe_info& info)
    {
        //std::cout << comm_world.rank() << ": " << "sending task data: " << tid.num << '_' << tid.proc << " to " << proc << std::endl;
        for (message_id i: memory.get_task_data(tid))
            send_message(i, proc, comm_workload, info);

        for (message_id i: memory.get_task_const_data(tid))
            send_message(i, proc, comm_workload, info);

        message_id m_tid = memory.get_task_base_message(tid);
        send_message(m_tid, proc, comm_workload, info);
    }

    void parallelizer::send_message(message_id id, size_t proc, std::vector<size_t>& comm_workload, master_exe_info& info)
    {
        std::function<process(std::vector<std::set<message_id>>&)> get_proc = [&](std::vector<std::set<message_id>>& search_sets)->process
        {
            std::set<process, std::function<bool(process, process)>> r_ver([&](process a, process b)->bool
            {
                if (comm_workload[a] != comm_workload[b])
                    return comm_workload[a] < comm_workload[b];
                return a < b;
            });

            for (size_t i = 0; i < info.process_workgroup.size(); ++i)
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

        if (info.contained_messages[proc].find(id) == info.contained_messages[proc].end())
        {
            sender_proc = get_proc(info.contained_messages);
            if (sender_proc == MPI_PROC_NULL)
                main_comm.abort(432);

            if (memory.get_message_factory_type(id) == MESSAGE_FACTORY_TYPE::CHILD)
                info.instructions_for_processes[proc].add_message_part_creation(id, memory.get_message_type(id), memory.get_message_parent(id), info.process_workgroup[sender_proc]);
            else
                info.instructions_for_processes[proc].add_message_creation(id, memory.get_message_type(id), info.process_workgroup[sender_proc]);
            info.contained_messages[proc].insert(id);
            info.instructions_for_processes[sender_proc].add_message_info_sending(id, info.process_workgroup[proc]);

            ++comm_workload[proc];
            ++comm_workload[sender_proc];
        }
        if (info.versions_of_messages[proc].find(id) == info.versions_of_messages[proc].end())
        {
            sender_proc = get_proc(info.versions_of_messages);
            if (sender_proc == MPI_PROC_NULL)
            {
                std::set<message_id>& ch = memory.get_message_childs(id);
                if (ch.size() == 0)
                    main_comm.abort(433);
                for (message_id i: ch)
                {
                    send_message(i, proc, comm_workload, info);
                    info.instructions_for_processes[proc].add_include_child_to_parent(id, i);
                    memory.set_message_child_state(i, CHILD_STATE::INCLUDED);
                }
            }
            else
            {
                if ((info.contained_messages[proc].find(memory.get_message_parent(id)) == info.contained_messages[proc].end()) || (info.versions_of_messages[proc].find(memory.get_message_parent(id)) == info.versions_of_messages[proc].end()))
                {
                    info.instructions_for_processes[proc].add_message_receiving(id, info.process_workgroup[sender_proc]);
                    info.instructions_for_processes[sender_proc].add_message_sending(id, info.process_workgroup[proc]);

                    ++comm_workload[proc];
                    ++comm_workload[sender_proc];
                }
            }
            info.versions_of_messages[proc].insert(id);
        }
    }

    void parallelizer::send_instruction(instruction& current_instruction)
    {
        for (const instruction_block& i: current_instruction)
        {
            switch (i.command())
            {
            case INSTRUCTION::MES_SEND:
            {
                const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                main_comm.isend(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
                break;
            }
            case INSTRUCTION::MES_INFO_SEND:
            {
                const instruction_message_info_send& j = dynamic_cast<const instruction_message_info_send&>(i);
                request_block& info_req = memory.get_message_info_request_block(j.id());
                for (message* p: memory.get_message_info(j.id()))
                    main_comm.isend(p, j.proc(), info_req);
                break;
            }
            case INSTRUCTION::MES_RECV:
            {
                const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                main_comm.irecv(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
                break;
            }
            case INSTRUCTION::MES_CREATE:
            {
                const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                request_block info_req;
                std::vector<message*> info_v = message_init_factory::get_info(j.type());
                for (message* p: info_v)
                    main_comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_init_with_id(j.id(), j.type(), info_v);
                break;
            }
            case INSTRUCTION::MES_P_CREATE:
            {
                const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                std::vector<message*> info_v = message_child_factory::get_info(j.type());
                request_block info_req;
                for (message* p: info_v)
                    main_comm.irecv(p, j.proc(), info_req);
                info_req.wait_all();
                memory.create_message_child_with_id(j.id(), j.type(), j.source(), info_v);
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
                main_comm.abort(555);
            }
        }
    }

    void parallelizer::end_main_task(size_t proc, task_id current_task_id, task_environment& env, std::vector<task_id>& tasks_to_del, master_exe_info& info)
    {
        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<message_id> messages_childs_id;
        std::vector<message_id> messages_childs_add_id;

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_task_data(current_task_id))
        {
            for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                info.versions_of_messages[k].erase(i);
            info.versions_of_messages[proc].insert(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                j = memory.get_message_parent(j);
                for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                    info.versions_of_messages[k].erase(j);
            }
        }

        message_id tid_base = memory.get_task_base_message(current_task_id);
        for (size_t k = 0; k < info.process_workgroup.size(); ++k)
            info.versions_of_messages[k].erase(tid_base);
        info.versions_of_messages[proc].insert(tid_base);

        for (const local_message_id& i: env.result_message_ids())
        {
            switch (i.src)
            {
                case MESSAGE_SOURCE::INIT:
                {
                    message_init_data& d = env.created_messages_init()[i.id];
                    messages_init_id.push_back(memory.create_message_init(d.type, d.ii));
                    info.contained_messages[proc].insert(messages_init_id.back());
                    info.versions_of_messages[proc].insert(messages_init_id.back());
                    break;
                }
                case MESSAGE_SOURCE::INIT_A:
                {
                    message_init_add_data& d = env.added_messages_init()[i.id];
                    messages_init_add_id.push_back(memory.add_message_init(d.mes, d.type, d.ii));
                    info.contained_messages[proc].insert(messages_init_add_id.back());
                    info.versions_of_messages[proc].insert(messages_init_add_id.back());
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
                            src = memory.get_task_data(current_task_id)[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = memory.get_task_const_data(current_task_id)[d.sourse.id];
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
                            main_comm.abort(767);
                    }
                    messages_childs_id.push_back(memory.create_message_child(d.type, src, d.pi));
                    info.contained_messages[proc].insert(messages_childs_id.back());
                    info.versions_of_messages[proc].insert(messages_childs_id.back());
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
                            src = memory.get_task_data(current_task_id)[d.sourse.id];
                            break;
                        }
                        case MESSAGE_SOURCE::TASK_ARG_C:
                        {
                            src = memory.get_task_const_data(current_task_id)[d.sourse.id];
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
                            main_comm.abort(767);
                    }
                    for (size_t k = 1; k < info.process_workgroup.size(); ++k)
                        info.versions_of_messages[k].erase(src);
                    messages_childs_add_id.push_back(memory.add_message_child(d.mes, d.type, src, d.pi));
                    info.contained_messages[proc].insert(messages_childs_add_id.back());
                    info.versions_of_messages[proc].insert(messages_childs_add_id.back());
                    break;
                }
                default:
                    main_comm.abort(767);
            }
        }

        size_t current_task_childs_count = 0;
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
                    ++current_task_childs_count;
                    break;
                }
                default:
                    main_comm.abort(767);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    mes_t_id = memory.get_task_data(current_task_id)[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = memory.get_task_const_data(current_task_id)[i.mes.id];
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
                    main_comm.abort(767);
            }

            std::vector<message_id> data_id;
            data_id.reserve(local_data.size());
            for (local_message_id k: local_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    {
                        data_id.push_back(memory.get_task_data(current_task_id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(memory.get_task_const_data(current_task_id)[k.id]);
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
                        main_comm.abort(767);
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
                        const_data_id.push_back(memory.get_task_data(current_task_id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(memory.get_task_const_data(current_task_id)[k.id]);
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
                        main_comm.abort(767);
                }
            }

            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    task_id id = memory.add_task(mes_t_id, t.type, data_id, const_data_id);
                    tasks_id.push_back(id);
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    task_id id = memory.add_task(mes_t_id, t.type, data_id, const_data_id);
                    tasks_child_id.push_back(id);
                    memory.set_task_parent(id, current_task_id);
                    break;
                }
                default:
                    main_comm.abort(767);
            }
        }
        memory.set_task_created_childs(current_task_id, memory.get_task_created_childs(current_task_id) + current_task_childs_count);

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
                    parent = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
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
                    child = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
            }
            memory.add_dependence(parent, child);
        }

        for (const separate_task_proc_count& i: env.separate_tasks())
        {
            task_id sep_task {};
            switch (i.id.src)
            {
                case TASK_SOURCE::INIT:
                {
                    sep_task = tasks_id[i.id.id];
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    sep_task = tasks_child_id[i.id.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    sep_task = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
            }
            memory.set_task_preferred_processes_count(sep_task, i.processes_count);
        }

        for (const task_id& i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push({ i, memory.get_task_preferred_processes_count(i) });
        }

        for (const task_id& i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push({ i, memory.get_task_preferred_processes_count(i) });
        }

        update_ready_tasks(current_task_id, tasks_to_del, info);
    }

    void parallelizer::wait_task(size_t proc, std::vector<task_id>& tasks_to_del, master_exe_info& info)
    {
        instruction result_instruction;
        instr_comm.recv<message>(&result_instruction, info.process_workgroup[proc]);

        instruction::const_iterator it = result_instruction.begin();
        const instruction_block& current_block = *it;

        if (current_block.command() != INSTRUCTION::TASK_RES)
            main_comm.abort(111);
        const instruction_task_result& result = dynamic_cast<const instruction_task_result&>(current_block);
        task_id current_task_id = result.id();

        std::vector<message_id> messages_init_id;
        std::vector<message_id> messages_init_add_id;
        std::vector<std::pair<message_id, message_id>> messages_childs_id;
        std::vector<std::pair<message_id, message_id>> messages_childs_add_id;

        const instruction_block& ins2 = *(++it);
        if (ins2.command() != INSTRUCTION::ADD_RES_TO_MEMORY)
            main_comm.abort(112);
        const instruction_add_result_to_memory& res_to_mem = dynamic_cast<const instruction_add_result_to_memory&>(ins2);
        messages_init_add_id = res_to_mem.added_messages_init();
        messages_childs_add_id = res_to_mem.added_messages_child();

        const instruction_block& ins3 = *(++it);
        if (ins3.command() != INSTRUCTION::ADD_RES_TO_MEMORY)
            main_comm.abort(113);
        const instruction_add_result_to_memory& res_to_mem2 = dynamic_cast<const instruction_add_result_to_memory&>(ins3);
        messages_init_id = res_to_mem2.added_messages_init();
        messages_childs_id = res_to_mem2.added_messages_child();

        result_instruction.clear();

        request_block res_ins_req;
        task_environment env;
        main_comm.irecv<message>(&env, info.process_workgroup[proc], res_ins_req);
        res_ins_req.wait_all();

        std::vector<task_id> tasks_id;
        std::vector<task_id> tasks_child_id;

        for (message_id i: memory.get_task_data(current_task_id))
        {
            for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                info.versions_of_messages[k].erase(i);
            info.versions_of_messages[proc].insert(i);
            message_id j = i;
            while (memory.message_has_parent(j))
            {
                if (memory.get_message_factory_type(j) == MESSAGE_FACTORY_TYPE::CHILD)
                    memory.set_message_child_state(j, CHILD_STATE::NEWER);
                j = memory.get_message_parent(j);
                for (size_t k = 0; k < info.process_workgroup.size(); ++k)
                    info.versions_of_messages[k].erase(j);
            }
        }

        message_id tid_base = memory.get_task_base_message(current_task_id);
        for (size_t k = 0; k < info.process_workgroup.size(); ++k)
            info.versions_of_messages[k].erase(tid_base);
        info.versions_of_messages[proc].insert(tid_base);

        std::vector<message_type> messages_init_id_type;
        std::vector<message_type> messages_init_add_id_type;
        std::vector<message_type> messages_childs_id_type;
        std::vector<message_type> messages_childs_add_id_type;

        for (const local_message_id& i: env.result_message_ids())
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
                    main_comm.abort(767);
            }
        }

        for (size_t i = 0; i < messages_init_id.size(); ++i)
        {
            info.contained_messages[proc].insert(messages_init_id[i]);
            info.versions_of_messages[proc].insert(messages_init_id[i]);
            memory.add_message_to_graph(messages_init_id[i], messages_init_id_type[i]);
        }

        for (size_t i = 0; i < messages_init_add_id.size(); ++i)
        {
            info.contained_messages[proc].insert(messages_init_add_id[i]);
            info.versions_of_messages[proc].insert(messages_init_add_id[i]);
            memory.add_message_to_graph(messages_init_add_id[i], messages_init_add_id_type[i]);
        }

        for (size_t i = 0; i < messages_childs_id.size(); ++i)
        {
            info.contained_messages[proc].insert(messages_childs_id[i].second);
            info.versions_of_messages[proc].insert(messages_childs_id[i].second);
            memory.add_message_child_to_graph(messages_childs_id[i].second, messages_childs_id_type[i], messages_childs_id[i].first);
            memory.insert_message_child(messages_childs_id[i].first, messages_childs_id[i].second);
        }

        for (size_t i = 0; i < messages_childs_add_id.size(); ++i)
        {
            info.contained_messages[proc].insert(messages_childs_add_id[i].second);
            info.versions_of_messages[proc].insert(messages_childs_add_id[i].second);
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
                    main_comm.abort(767);
            }

            switch (i.mes.src)
            {
                case MESSAGE_SOURCE::TASK_ARG:
                {
                    mes_t_id = memory.get_task_data(current_task_id)[i.mes.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    mes_t_id = memory.get_task_const_data(current_task_id)[i.mes.id];
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
                    main_comm.abort(767);
            }

            std::vector<message_id> data_id;
            data_id.reserve(local_data.size());
            for (local_message_id k: local_data)
            {
                switch (k.src)
                {
                    case MESSAGE_SOURCE::TASK_ARG:
                    {
                        data_id.push_back(memory.get_task_data(current_task_id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        data_id.push_back(memory.get_task_const_data(current_task_id)[k.id]);
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
                        main_comm.abort(767);
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
                        const_data_id.push_back(memory.get_task_data(current_task_id)[k.id]);
                        break;
                    }
                    case MESSAGE_SOURCE::TASK_ARG_C:
                    {
                        const_data_id.push_back(memory.get_task_const_data(current_task_id)[k.id]);
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
                        main_comm.abort(767);
                }
            }

            switch (i.src)
            {
                case TASK_SOURCE::INIT:
                {
                    task_data& t = env.created_tasks_simple()[i.id];
                    task_id id = memory.add_task(mes_t_id, t.type, data_id, const_data_id);
                    tasks_id.push_back(id);
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    task_data& t = env.created_child_tasks()[i.id];
                    task_id id = memory.add_task(mes_t_id, t.type, data_id, const_data_id);
                    tasks_child_id.push_back(id);
                    memory.set_task_parent(id, current_task_id);
                    break;
                }
                default:
                    main_comm.abort(767);
            }
        }
        memory.set_task_created_childs(current_task_id, memory.get_task_created_childs(current_task_id) + tid_childs);

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
                    parent = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
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
                    child = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
            }

            memory.add_dependence(parent, child);
        }

        for (const separate_task_proc_count& i: env.separate_tasks())
        {
            task_id sep_task{};
            switch (i.id.src)
            {
                case TASK_SOURCE::INIT:
                {
                    sep_task = tasks_id[i.id.id];
                    break;
                }
                case TASK_SOURCE::CHILD:
                {
                    sep_task = tasks_child_id[i.id.id];
                    break;
                }
                case TASK_SOURCE::GLOBAL:
                {
                    sep_task = current_task_id;
                    break;
                }
                default:
                    main_comm.abort(767);
            }
            memory.set_task_preferred_processes_count(sep_task, i.processes_count);
        }

        for (const task_id& i: tasks_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push({ i, memory.get_task_preferred_processes_count(i) });
        }

        for (const task_id& i: tasks_child_id)
        {
            if (memory.get_task_parents_count(i) == 0)
                info.ready_tasks.push({ i, memory.get_task_preferred_processes_count(i) });
        }

        update_ready_tasks(current_task_id, tasks_to_del, info);
    }

    void parallelizer::update_ready_tasks(task_id current_task_id, std::vector<task_id>& tasks_to_del, master_exe_info& info)
    {
        task_id current_id = current_task_id;
        while (1)
        {
            if (memory.get_task_created_childs(current_id) == 0)
            {
                for (task_id i: memory.get_task_childs(current_id))
                {
                    memory.set_task_parents_count(i, memory.get_task_parents_count(i) - 1);
                    if (memory.get_task_parents_count(i) == 0)
                        info.ready_tasks.push({ i, memory.get_task_preferred_processes_count(i)});
                }
                tasks_to_del.push_back(current_id);
            }
            else
                break;
            if (!memory.task_has_parent(current_id))
                break;
            else
            {
                current_id = memory.get_task_parent(current_id);
                memory.set_task_created_childs(current_id, memory.get_task_created_childs(current_id) - 1);
            }
        }
    }

    void parallelizer::worker(size_t current_processes_count)
    {
        //std::cout << comm_world.rank() << ": " << "started_worker_exe" << std::endl;
        instruction cur_inst;
        while(1)
        {
            bool queue_try = false;
            bool comm_try = false;
            //std::cout << comm_world.rank() << ": " << "worker_cycle" << std::endl;

            if (instr_comm.test_process(main_proc))
            {
                //std::cout << comm_world.rank() << ": " << "start instriction_receiving" << std::endl;
                instr_comm.recv<message>(&cur_inst, main_proc);
                //std::cout << comm_world.rank() << ": " << "instriction_received: ";
                //for (size_t i = 0; i < cur_inst.size(); ++i)
                //{
                //    std::cout << cur_inst[i] << ' ';
                //}
                //std::cout << std::endl;

                for (const instruction_block& i: cur_inst)
                {
                    //std::cout << comm_world.rank() << ": " << "command start: " << static_cast<size_t>(i.command()) << std::endl;
                    switch (i.command())
                    {
                    case INSTRUCTION::MES_SEND:
                    {
                        const instruction_message_send& j = dynamic_cast<const instruction_message_send&>(i);
                        main_comm.isend(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
                        break;
                    }
                    case INSTRUCTION::MES_RECV:
                    {
                        const instruction_message_recv& j = dynamic_cast<const instruction_message_recv&>(i);
                        main_comm.irecv(memory.get_message(j.id()), j.proc(), memory.get_message_request_block(j.id()));
                        break;
                    }
                    case INSTRUCTION::MES_INFO_SEND:
                    {
                        const instruction_message_info_send& j = dynamic_cast<const instruction_message_info_send&>(i);
                        request_block& info_req = memory.get_message_info_request_block(j.id());
                        for (message* p: memory.get_message_info(j.id()))
                            main_comm.isend(p, j.proc(), info_req);
                        break;
                    }
                    case INSTRUCTION::MES_CREATE:
                    {
                        const instruction_message_create& j = dynamic_cast<const instruction_message_create&>(i);
                        std::vector<message*> info_v = message_init_factory::get_info(j.type());
                        request_block info_req;
                        for (message* p: info_v)
                            main_comm.irecv(p, j.proc(), info_req);
                        info_req.wait_all();
                        memory.create_message_init_with_id(j.id(), j.type(), info_v);
                        break;
                    }
                    case INSTRUCTION::MES_P_CREATE:
                    {
                        const instruction_message_part_create& j = dynamic_cast<const instruction_message_part_create&>(i);
                        std::vector<message*> info_v = message_child_factory::get_info(j.type());
                        request_block info_req;
                        for (message* p: info_v)
                            main_comm.irecv(p, j.proc(), info_req);
                        info_req.wait_all();
                        memory.create_message_child_with_id(j.id(), j.type(), j.source(), info_v);
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
                        memory.add_task_with_id(j.id(), j.base_id(), j.type(), j.data(), j.const_data());
                        //std::cout << comm_world.rank() << ": " << "added_task_with_id: " << j.id().num << '_' << j.id().proc << std::endl;
                        break;
                    }
                    case INSTRUCTION::TASK_EXE:
                    {
                        const instruction_task_execute& j = dynamic_cast<const instruction_task_execute&>(i);
                        execute_task(j.id());
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
                        memory.delete_task(j.id());
                        break;
                    }
                    case INSTRUCTION::CHANGE_OWNER:
                    {
                        const instruction_change_owner& j = dynamic_cast<const instruction_change_owner&>(i);
                        main_proc = j.new_owner();
                        current_processes_count = j.new_processes_count();
                        break;
                    }
                    case INSTRUCTION::INDEPENDENT_EXE:
                    {
                        const instruction_independent_exe& j = dynamic_cast<const instruction_independent_exe&>(i);
                        //std::cout << comm_world.rank() << ": " << "started_independent_exe" << std::endl;
                        master_exe_info info;
                        main_comm.recv<message>(&info, main_proc);
                        info.ready_tasks.push({j.start_task(), memory.get_task_preferred_processes_count(j.start_task())});
                        //info.ready_tasks = std::move(memory.get_ready_tasks());

                        //std::cout << comm_world.rank() << ": " << "master_exe_info_sizes: " << info.process_workgroup.size() << ' ' << info.contained_messages.size() << ' '
                        //    << info.versions_of_messages.size() << ' ' << info.process_positions.size() << ' ' << info.instructions_for_processes.size() << std::endl;

                        master(info);

                        {
                            instruction end;
                            end.add_change_owner(main_proc, current_processes_count);
                            for (size_t i = 1; i < info.process_workgroup.size(); ++i)
                                instr_comm.send<message>(&end, info.process_workgroup[i]);
                        }

                        //std::cout << comm_world.rank() << ": " << "start_sending_to_previous_master: " << main_proc << std::endl;
                        {
                            instruction end;

                            std::function<void(message_id)> graph_recursive_f = [&](message_id cur_id)->void
                            {
                                end.add_packed_message_graph(cur_id, memory.get_message_type(cur_id), memory.get_message_factory_type(cur_id), memory.get_message_parent(cur_id),
                                    memory.get_message_childs(cur_id), memory.get_message_refs_count(cur_id), memory.get_message_child_state(cur_id));
                                const std::set<message_id>& s = memory.get_message_childs(cur_id);
                                for (message_id k: s)
                                    graph_recursive_f(k);
                            };

                            for (message_id k: memory.get_task_data(j.start_task()))
                            {
                                graph_recursive_f(k);
                            }
                            graph_recursive_f(memory.get_task_base_message(j.start_task()));
                            instr_comm.send<message>(&end, main_proc);
                        }
                        //std::cout << comm_world.rank() << ": " << "start_sending_info_master: " << main_proc << std::endl;
                        main_comm.send<message>(&info, main_proc);
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

                finished_task_execution_queue_data current_output_data{ current_execution_data.this_task_id, {current_execution_data.this_task_type,
                    current_execution_data.args.size(), current_execution_data.const_args.size(), current_processes_count, execution_thread_count} };
                current_execution_data.this_task->set_environment(&current_output_data.this_task_environment);
                task_factory::perform(current_execution_data.this_task_type, current_execution_data.this_task, current_execution_data.args, current_execution_data.const_args);
                current_execution_data.this_task->set_environment(nullptr);

                finished_task_queue_mutex.lock();
                finished_task_queue.push(std::move(current_output_data));
                finished_task_queue_mutex.unlock();
            }
        }
        end:;
    }

    void parallelizer::worker_task_finishing(finished_task_execution_queue_data& cur_task_exe_data)
    {
        std::vector<message_id> added_m_init, messages_init_id;
        std::vector<std::pair<message_id, message_id>> added_m_child, messages_childs_id;
        for (const local_message_id& i: cur_task_exe_data.this_task_environment.result_message_ids())
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
                    src = memory.get_task_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    src = memory.get_task_const_data(cur_task_exe_data.this_task_id)[d.sourse.id];
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
                    main_comm.abort(767);
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
                    src = memory.get_task_data(cur_task_exe_data.this_task_id)[d.sourse.id];
                    break;
                }
                case MESSAGE_SOURCE::TASK_ARG_C:
                {
                    src = memory.get_task_const_data(cur_task_exe_data.this_task_id)[d.sourse.id];
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
                    main_comm.abort(767);
                }
                messages_childs_id.push_back({ src, memory.create_message_child(d.type, src, d.pi) });
                break;
            }
            default:
                main_comm.abort(767);
            }
        }

        instruction res;
        res.add_task_result(cur_task_exe_data.this_task_id);
        res.add_add_result_to_memory(added_m_init, added_m_child);
        res.add_add_result_to_memory(messages_init_id, messages_childs_id);

        request_block req;
        instr_comm.send<message>(&res, main_proc);
        main_comm.isend<message>(&cur_task_exe_data.this_task_environment, main_proc, req);
        memory.delete_task(cur_task_exe_data.this_task_id);
        req.wait_all();
    }

    void parallelizer::execute_task(task_id id)
    {
        task_execution_queue_data current_data{ id, memory.get_message_as_task(memory.get_task_base_message(id)), memory.get_task_type(id) };
        for (message_id j: memory.get_task_data(id))
            current_data.args.push_back(memory.get_message(j));
        for (message_id j: memory.get_task_const_data(id))
            current_data.const_args.push_back(memory.get_message(j));

        task_queue_mutex.lock();
        task_queue.push(std::move(current_data));
        task_queue_mutex.unlock();

        {
            std::unique_lock<std::mutex> cv_lk(task_queue_mutex);
            exe_threads_cv.notify_all();
        }
    }

    process parallelizer::get_current_proc()
    { return main_comm.rank(); }

    size_t parallelizer::get_workers_count()
    { return main_comm.size() * execution_thread_count; }

    void parallelizer::clear()
    { memory.clear(); }

}
