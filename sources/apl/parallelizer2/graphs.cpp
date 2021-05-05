#include "apl/parallelizer2/graphs.h"

namespace apl
{

    template<>
    void sender::send<message_graph_node>(const message_graph_node& src) const
    {
        send_bytes(&src, sizeof(message_type) + sizeof(MESSAGE_FACTORY_TYPE) + sizeof(message_id));
        send(src.childs.size());
        for (auto& i: src.childs)
            send(i);
        send_bytes(&src.refs_count, sizeof(size_t) + sizeof(CHILD_STATE) + sizeof(process));
    }

    template<>
    void receiver::recv<message_graph_node>(message_graph_node& dst) const
    {
        recv_bytes(&dst, sizeof(message_type) + sizeof(MESSAGE_FACTORY_TYPE) + sizeof(message_id));
        size_t u;
        recv(u);
        dst.childs.clear();
        for (size_t i = 0; i < u; ++i)
        {
            message_id g;
            recv(g);
            dst.childs.insert(std::move(g));
        }
        recv_bytes(&dst.refs_count, sizeof(size_t) + sizeof(CHILD_STATE) + sizeof(process));
    }

    template<>
    void sender::send<task_graph_node>(const task_graph_node& src) const
    {
        send_bytes(&src.m_id, sizeof(message_id) + sizeof(perform_type) + sizeof(perform_id) + 2 * sizeof(size_t));
        send(src.owner);
        send(src.childs_v.data(), src.childs_v.size());
        send(src.data.data(), src.data.size());
        send(src.const_data.data(), src.const_data.size());
    }

    template<>
    void receiver::recv<task_graph_node>(task_graph_node& dst) const
    {
        recv_bytes(&dst.m_id, sizeof(message_id) + sizeof(perform_type) + sizeof(perform_id) + 2 * sizeof(size_t));
        recv(dst.owner);
        dst.childs_v.resize(probe<perform_id>());
        recv(dst.childs_v.data(), dst.childs_v.size());
        dst.childs_v.resize(probe<perform_id>());
        recv(dst.data.data(), dst.data.size());
        dst.childs_v.resize(probe<perform_id>());
        recv(dst.const_data.data(), dst.const_data.size());
    }

    template<>
    void sender::send<sub_graph>(const sub_graph& src) const
    {
        send(src.tasks.data(), src.tasks.size());
        send(src.ins.data(), src.ins.size());
        send(src.outs.data(), src.outs.size());
        send(src.child_ins.data(), src.child_ins.size());
        send(src.child_outs.data(), src.child_outs.size());
    }

    template<>
    void receiver::recv<sub_graph>(sub_graph& dst) const
    {
        dst.tasks.resize(probe<perform_id>());
        recv(dst.tasks.data(), dst.tasks.size());
        dst.ins.resize(probe<graph_connection>());
        recv(dst.ins.data(), dst.ins.size());
        dst.outs.resize(probe<graph_connection>());
        recv(dst.outs.data(), dst.outs.size());
        dst.child_ins.resize(probe<graph_connection>());
        recv(dst.child_ins.data(), dst.child_ins.size());
        dst.child_outs.resize(probe<graph_connection>());
        recv(dst.child_outs.data(), dst.child_outs.size());
    }

    graph_analizer::graph_analizer(const graph_adapter& a): adapter(std::addressof(a))
    { }

    bool graph_analizer::need_split(const std::vector<processes_group>& groups, const std::vector<group_info>& info, size_t ready_tasks_size) const
    {
        for (size_t i = 0; i < groups.size(); ++i)
        {
            size_t sz = groups[i].size - std::min(info[i].active_tasks, groups[i].size);
            if (sz > 1)
                return true;
        }
        return false;
    }

    std::vector<sub_graph> graph_analizer::split_graph(const std::vector<processes_group>& groups, const std::vector<group_info>& info, std::deque<perform_id>& ready_tasks) const
    {
        std::vector<sub_graph> sub_graphs;
        // add implementation
        return sub_graphs;
    }

    bool graph_connection::operator<(const graph_connection& oth) const
    {
        if (out != oth.out)
            return out < oth.out;
        if (in != oth.in)
            return in < oth.in;
        return external_group_id < oth.external_group_id;
    }

}
