#ifndef __VECTOR_MAP_H__
#define __VECTOR_MAP_H__

#include <map>
#include <vector>
#include <limits>

template<typename Key, typename Data, typename MapType = std::map<const Key, size_t>>
class vector_map
{
private:

    struct node
    {
        Data data;
        size_t next = 0;
    };

    MapType m;
    std::vector<node> c;
    size_t first_free_node = std::numeric_limits<size_t>::max();

    size_t acquire_free_node();
    void unref_node(size_t n);
    void free_node(size_t n);

public:

    class iterator
    {
    private:
        typename MapType::iterator m_it;
        node* vec;
        

    public:
        iterator();
        ~iterator();

        bool operator==(const iterator& it) const;
        bool operator!=(const iterator& it) const;
        iterator& operator++();
        const Key& key() const;
        Data* operator->() const;
        Data& operator*() const;

        friend class vector_map;
    };

    typedef std::pair<const Key, Data> value_type;
    typedef Key key_type;
    typedef Data mapped_type;

    vector_map();
    ~vector_map();

    void insert(const value_type& val);
    void erase(const Key& val);
    void destroy(const Key& val);
    bool contains(const Key& val) const;

    Data& operator[](const Key& val);

    size_t size() const;

    iterator begin();
    iterator end();

    void clear();

};

template<typename Key, typename Data, typename MapType>
vector_map<Key, Data, MapType>::iterator::iterator(): vec(nullptr)
{ }

template<typename Key, typename Data, typename MapType>
vector_map<Key, Data, MapType>::iterator::~iterator()
{ }

template<typename Key, typename Data, typename MapType>
bool vector_map<Key, Data, MapType>::iterator::operator==(const iterator & it) const
{
    if (m_it != it.m_it)
        return false;
    return vec == it.vec;
}

template<typename Key, typename Data, typename MapType>
bool vector_map<Key, Data, MapType>::iterator::operator!=(const iterator& it) const
{
    if (m_it != it.m_it)
        return true;
    return vec != it.vec;
}

template<typename Key, typename Data, typename MapType>
typename vector_map<Key, Data, MapType>::iterator& vector_map<Key, Data, MapType>::iterator::operator++()
{
    ++m_it;
    return *this;
}

template<typename Key, typename Data, typename MapType>
const Key& vector_map<Key, Data, MapType>::iterator::key() const
{ return m_it->first; }

template<typename Key, typename Data, typename MapType>
Data* vector_map<Key, Data, MapType>::iterator::operator->() const
{ return &((vec + m_it->second)->data); }

template<typename Key, typename Data, typename MapType>
Data& vector_map<Key, Data, MapType>::iterator::operator*() const
{ return (vec + m_it->second)->data; }

template<typename Key, typename Data, typename MapType>
vector_map<Key, Data, MapType>::vector_map()
{ }

template<typename Key, typename Data, typename MapType>
vector_map<Key, Data, MapType>::~vector_map()
{ }

template<typename Key, typename Data, typename MapType>
size_t vector_map<Key, Data, MapType>::acquire_free_node()
{
    if (first_free_node == std::numeric_limits<size_t>::max())
    {
        c.resize(c.size() + 1);
        return c.size() - 1;
    }
    else
    {
        size_t ret_node = first_free_node;
        first_free_node = c[first_free_node].next;
        return ret_node;
    }
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::unref_node(size_t n)
{
    c[n].next = first_free_node;
    first_free_node = n;
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::free_node(size_t n)
{
    c[n].data.~Data();
    c[n].next = first_free_node;
    first_free_node = n;
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::insert(const value_type& val)
{
    if (m.find(val.first) == m.end())
    {
        size_t current_node = acquire_free_node();
        m.insert({val.first, current_node});
        c[current_node].data = val.second;
    }
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::erase(const Key& val)
{
    auto it = m.find(val);
    if (it != m.end())
    {
        unref_node(it->second);
        m.erase(it);
    }
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::destroy(const Key& val)
{
    auto it = m.find(val);
    if (it != m.end())
    {
        free_node(it->second);
        m.erase(it);
    }
}

template<typename Key, typename Data, typename MapType>
bool vector_map<Key, Data, MapType>::contains(const Key& val) const
{ return m.find(val) != m.end(); }

template<typename Key, typename Data, typename MapType>
Data& vector_map<Key, Data, MapType>::operator[](const Key& val)
{
    auto it = m.find(val);
    if (it == m.end())
    {
        size_t current_node = acquire_free_node();
        it = m.insert({val, current_node}).first;
    }
    return c[it->second].data;
}

template<typename Key, typename Data, typename MapType>
size_t vector_map<Key, Data, MapType>::size() const
{ return m.size(); }

template<typename Key, typename Data, typename MapType>
typename vector_map<Key, Data, MapType>::iterator vector_map<Key, Data, MapType>::begin()
{
    iterator i;
    i.m_it = m.begin();
    i.vec = c.data();
    return i;
}

template<typename Key, typename Data, typename MapType>
typename vector_map<Key, Data, MapType>::iterator vector_map<Key, Data, MapType>::end()
{
    iterator i;
    i.m_it = m.end();
    i.vec = c.data();
    return i;
}

template<typename Key, typename Data, typename MapType>
void vector_map<Key, Data, MapType>::clear()
{
    m.clear();
    c.clear();
    first_free_node = std::numeric_limits<size_t>::max();
}

#endif // __VECTOR_MAP_H__
