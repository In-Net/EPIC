#include <sstream>

#include <argparse/argparse.hpp>
// TODO: add {fmt}
#include <boost/algorithm/string/join.hpp>

#include "common.hpp"

vector<int> cpu_list_on_node(int node, int thread_cnt) 
{
    vector<int> cpu_list = get_cpu_list_by_socket(node);
    assert(!cpu_list.empty());

    assert(thread_cnt <= (int)cpu_list.size() && thread_cnt > 0);


    std::reverse(cpu_list.begin(), cpu_list.end());// cpu 20~39 has strange performance, skip them


    cpu_list.resize(thread_cnt);
    return cpu_list;
}

string cpu_list_to_string(vector<int> cpu_list)
{
    vector<string> cpu_string_list;
    for(auto cpu: cpu_list) cpu_string_list.push_back(std::to_string(cpu));
    string cpu_string = boost::join(cpu_string_list, ",");
    return cpu_string;
}

static int
port_init(uint16_t port, int nqueues, struct rte_mempool **mbuf_pool)
{
    if(nqueues == 0) {
        spdlog::warn("port_init() with zero queue");
        return 0;
    }
    struct rte_eth_conf port_conf;
    const uint16_t rx_queues = nqueues, tx_queues = nqueues;
    // Hardware RX queue size can be as large as possible to reduce packet loss.
    // Since RX has a higher priority than TX in our software implementation,
    // congestion will not occur in the RX queue
    uint16_t nb_rxd = MAX_WIN_LEN;
    // We must limit the hardware TX queue size either by rte_eth_tx_queue_setup or by rte_eth_tx_queue_count
    // to avoid congestions in TX queue, which lead to the increase in inflight packets, harmful to INC.
#ifdef rte_eth_tx_queue_count
    uint16_t nb_txd = MAX_WIN_LEN;
#else 
    uint16_t nb_txd = 2 * MAX_PKT_BURST;
#endif
    int retval;
    uint16_t q;
    struct rte_eth_dev_info dev_info;
    struct rte_eth_txconf txconf;

    if (!rte_eth_dev_is_valid_port(port)) {
        spdlog::error("Not a valid port");
        return -1;
    }

    memset(&port_conf, 0, sizeof(struct rte_eth_conf));
    // port_conf.rxmode.mtu = RTE_ETHER_MAX_LEN;

    retval = rte_eth_dev_info_get(port, &dev_info);
    assert(retval == 0);

#if RTE_VER_YEAR == 20
    if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
        port_conf.txmode.offloads |= DEV_TX_OFFLOAD_MBUF_FAST_FREE;
#elif RTE_VER_YEAR == 22
    if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
        port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;
#else 
    #error Untested version
#endif

    /* Configure the Ethernet device. */
    retval = rte_eth_dev_configure(port, rx_queues, tx_queues, &port_conf);
    assert(retval == 0);

    // rte_eth_dev_set_mtu

    retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
    spdlog::info("Number of packet descriptor: TX {}, RX {}", nb_txd, nb_rxd);
    assert(retval == 0);

    /* Allocate and set up 1 RX queue per Ethernet port. */
    for (q = 0; q < rx_queues; q++) {
        retval = rte_eth_rx_queue_setup(port, q, nb_rxd,
                rte_eth_dev_socket_id(port), NULL, mbuf_pool[q]);
        assert(retval == 0);
    }

    txconf = dev_info.default_txconf;
    txconf.offloads = port_conf.txmode.offloads;
    /* Allocate and set up 1 TX queue per Ethernet port. */
    for (q = 0; q < tx_queues; q++) {
        retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                rte_eth_dev_socket_id(port), &txconf);
        assert(retval == 0);
    }

    /* Starting Ethernet port. 8< */
    retval = rte_eth_dev_start(port);
    assert(retval == 0);
    /* >8 End of starting of ethernet port. */

    /* Display the port MAC address. */
    struct rte_ether_addr addr;
    retval = rte_eth_macaddr_get(port, &addr);
    assert(retval == 0);        

    // spdlog::info("Port {} MAC: {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
    //     port, RTE_ETHER_ADDR_BYTES(&addr));
    spdlog::info("Port {}", port);
    /* Enable RX in promiscuous mode for the Ethernet device. */
    retval = rte_eth_promiscuous_enable(port);
    /* End of setting RX port in promiscuous mode. */
    assert(retval == 0);
    return 0;
}


struct rte_flow* flow_create(uint16_t port_id, const struct rte_flow_attr *attr, const struct rte_flow_item pattern[], const struct rte_flow_action actions[])
{
    int ret;
    struct rte_flow_error err{};
    struct rte_flow *flow;
    ret = rte_flow_validate(port_id, attr, pattern, actions, &err);
    static std::map<int, string> errorcode = {{-ENOSYS, "ENOSYS"}, {-EIO, "EIO"}, {-EINVAL, "EINVAL"}, {-ENOTSUP, "ENOTSUP"}, {-EEXIST, "EEXIST"}, {-ENOMEM, "ENOMEM"}, {-EBUSY, "EBUSY"}};
    if(ret != 0) {
        spdlog::error("Flow validation failed ({}): {}", errorcode[ret], err.message);
        return NULL;
    }
    flow = rte_flow_create(port_id, attr, pattern, actions, &err);
    if(flow == NULL) {
        spdlog::error("Flow creation failed ({}): {}", errorcode[ret], err.message);
        return NULL;
    }
    return flow;
}

struct rte_flow* flow_init_queue(uint16_t port_id, uint16_t queue_id)
{   
    struct rte_flow_attr attr{};
    vector<struct rte_flow_item>pattern;
    vector<struct rte_flow_action>actions;

    // struct rte_flow_item_raw raw_spec{};
    // struct rte_flow_item_eth eth_spec{}, eth_mask{};
    struct rte_flow_item_ipv4 ip_spec{}, ip_mask{};
    // struct rte_flow_item_udp udp_spec{}, udp_mask{};
    struct rte_flow_action_queue action_queue{};

    static bool drop = 1;
    if(drop) {
        drop = 0;
        attr.ingress = 1;
        attr.priority = 1;
        // action_queue.index = 1;
        pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_END});
        actions.push_back({.type = RTE_FLOW_ACTION_TYPE_DROP});
        // actions.push_back({.type = RTE_FLOW_ACTION_TYPE_QUEUE, .conf = &action_queue});
        actions.push_back({.type = RTE_FLOW_ACTION_TYPE_END});
        flow_create(port_id, &attr, pattern.data(), actions.data());
        attr = {};
        pattern = {};
        actions = {};
        // action_queue = {};
    }

    attr.ingress = 1;
    
    // raw_spec.offset = OFFSETOF(netccl_pkt, inc.queue_id);
    // raw_spec.length = SIZEOF(netccl_pkt, inc.queue_id);
    // assert(raw_spec.length == 1 && queue_id < 256);
    // string byte_pattern;
    // byte_pattern.resize(1);
    // byte_pattern[0] = (char)queue_id;
    // raw_spec.pattern = (uint8_t*)byte_pattern.data();

    /*
    // RTE_FLOW_ITEM_TYPE_RAW is not supported for mlx5, let's use IPV4 as RAW
    size_t qid_offset = offset_of_member(netccl_pkt, inc.queue_id);
    size_t qid_size = size_of_member(netccl_pkt, inc.queue_id);
    assert(qid_offset >= sizeof(rte_ether_hdr) + sizeof(rte_ipv4_hdr));
    assert(qid_offset + qid_size <= sizeof(rte_ether_hdr) + sizeof(rte_ipv4_hdr) + sizeof(rte_udp_hdr));

    // size_t qid_header_offset = qid_offset - sizeof(rte_ether_hdr);
    size_t qid_header_offset = qid_offset - sizeof(rte_ether_hdr) - sizeof(rte_ipv4_hdr);

    assert(qid_size == 1);// otherwise, need byteswap
    auto header_qid = (type_of_member(netccl_pkt, inc.queue_id)) queue_id;
    memcpy((char*)&udp_spec + qid_header_offset, &header_qid, qid_size);
    memset((char*)&udp_mask + qid_header_offset, 0xff, qid_size);
    */

    // supported fields of mlx5 NIC:
    // IPv4: version, ihl, tos, fragment_offset, time_to_live, next_proto_id, src_addr, dst_addr
    // UDP: src_port, dst_port
    // NOTE: Not all of them support partial mask
    // NOTE: NIC has additional check for some packet fields even if they are not in `pattern`, E.g., 
    //       1. ip.version_ihl should be 0x45 
    //       2. if ip.next_proto_id == IPPROTO_UDP/TCP, then the NIC will check UDP/TCP header

    htobe(&ip_spec.hdr.type_of_service, queue_id << 2);
    htobe(&ip_mask.hdr.type_of_service, 0xfc);
    htobe(&ip_spec.hdr.next_proto_id, IPPROTO_NETCCL);
    htobe(&ip_mask.hdr.next_proto_id, 0xff);
    // htobe(&ip_spec.hdr.src_addr, 0xc0a80101);
    // htobe(&ip_mask.hdr.src_addr, 0xffffffff);
    // htobe(&ip_spec.hdr.dst_addr, 0xc0a801fe);
    // htobe(&ip_mask.hdr.dst_addr, 0xffffffff);
    // htobe(&udp_spec.hdr.dst_port, 5000 + queue_id);
    // htobe(&udp_mask.hdr.dst_port, 0xffff);

    pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_ETH});
    pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_IPV4, .spec = &ip_spec, .mask = &ip_mask});
    // pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_IPV4});
    // pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_UDP, .spec = &udp_spec, .mask = &udp_mask});
    // pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_UDP});
    pattern.push_back({.type = RTE_FLOW_ITEM_TYPE_END});

    action_queue.index = queue_id;
    // action_queue.index = queue_id ^ 1;
    actions.push_back({.type = RTE_FLOW_ACTION_TYPE_QUEUE, .conf = &action_queue});
    actions.push_back({.type = RTE_FLOW_ACTION_TYPE_END});
    // actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
    // actions[0].conf = &action_queue;

    // actions[1].type = RTE_FLOW_ACTION_TYPE_END;

    return flow_create(port_id, &attr, pattern.data(), actions.data());
}

int flow_init(uint16_t port_id, uint16_t nqueue)
{
    for(auto q = 0; q < nqueue; q++) {
        auto f = flow_init_queue(port_id, q);
        if(f == NULL) return -1;
    }
    return 0;
}

void dpdk_init(string ip)
{
    int retval;
    // get nic
    string dev = get_dev_by_ip(ip);
    assert(!dev.empty());

    // get node
    string pci = get_pci_by_dev(dev);
    assert(!pci.empty());
    int node = get_socket_by_pci(pci);
    assert(node != -1);

    // get cpu
    auto cpu_list = cpu_list_on_node(node, nthread);
    auto cpu_string = cpu_list_to_string(cpu_list);
    auto lcores_arg = string("(0-") + std::to_string(cpu_list.size()-1) + ")@(" + cpu_string + ")"; 

    // init eal
    vector<string>eal_parameters = {"netccl", "--in-memory", "--lcores", lcores_arg};//"-l", cpu_string};
    spdlog::info("EAL parameters: {}", boost::join(eal_parameters, " "));

    vector<char*>eal_parameters_char;
    for(auto &p: eal_parameters) eal_parameters_char.push_back(p.data());
    // eal_parameters_char.push_back(NULL);

    retval = rte_eal_init((int)eal_parameters_char.size(), eal_parameters_char.data());// note: DPDK will modify argv
    assert(retval >= 0);// == eal_parameters.size() - 1
    spdlog::info("EAL init success");

    // init pool
    vector<struct rte_mempool *>tx_mbuf_pool_list, rx_mbuf_pool_list;
    string pool_prefix = string("MBUF_") + ip + "_";
    for(int i = 0; i < nworker; i++) {
        string pool_name = pool_prefix + std::to_string(i);
        // use RTE_MBUF_DEFAULT_BUF_SIZE, refer to rte_mbuf_core.h
        string tx_pool_name = "TX_" + pool_name;
        string rx_pool_name = "RX_" + pool_name;

        struct rte_mempool *tx_mbuf_pool = rte_pktmbuf_pool_create(tx_pool_name.data(), MAX_WIN_LEN, 0, 0, RTE_PKTMBUF_HEADROOM + sizeof(netccl_pkt), rte_socket_id());
        struct rte_mempool *rx_mbuf_pool = rte_pktmbuf_pool_create(rx_pool_name.data(), MAX_WIN_LEN, RX_BUFFER_CACHE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

        assert(tx_mbuf_pool != NULL);
        assert(rx_mbuf_pool != NULL);

        tx_mbuf_pool_list.push_back(tx_mbuf_pool);
        rx_mbuf_pool_list.push_back(rx_mbuf_pool);
    }
    // struct rte_mempool *mbuf_pool = rte_pktmbuf_pool_create("MBUF_POOL", NUM_MBUFS * nthread, MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    // assert(mbuf_pool != NULL);

    // for(int i = 0; i < nthread; i++) {
    //     thread_ctx_t tc;
    //     thread_ctx.push_back(tc);
    // }

    // init port
    uint16_t nports = rte_eth_dev_count_avail();
    assert(nports > 0);
    uint16_t port;
    retval = rte_eth_dev_get_port_by_name(pci.data(), &port);// either name or pci address is OK?
    assert(retval == 0);
    spdlog::info("{} available ports, choose port {}", nports, port);

    retval = port_init(port, nworker, rx_mbuf_pool_list.data());
    assert(retval == 0);

    // init flow
    retval = flow_init(port, nworker);
    assert(retval == 0);


    // fill eth and ip headers of all packets
    // Maybe we can use rte_pktmbuf_chain to reduce memory usage? Does it hurt performance?
    netccl_prefill_pkt prefill_pkt;
    htobe(&prefill_pkt.ip.version_ihl, 0x45);
    htobe(&prefill_pkt.ip.type_of_service, 0);// to be filled later in for loop
    htobe(&prefill_pkt.ip.total_length, 0);// to be filled later in fill_pkt()
    htobe(&prefill_pkt.ip.packet_id, 0);// ignore
    htobe(&prefill_pkt.ip.fragment_offset, 0x4000);// don't fragment
    htobe(&prefill_pkt.ip.time_to_live, 0x40);// 64
    htobe(&prefill_pkt.ip.next_proto_id, IPPROTO_NETCCL);// do not use IPPROTO_UDP, otherwise NIC will check UDP header
    htobe(&prefill_pkt.ip.hdr_checksum, 0);// ignore
    htobe(&prefill_pkt.ip.src_addr, ipv4_to_int(ip));
    htobe(&prefill_pkt.ip.dst_addr, 0);// to be filled later in fill_pkt()
#if RTE_VER_YEAR == 20
    rte_ether_unformat_addr(SWITCH_MAC, &prefill_pkt.eth.d_addr);
    rte_eth_macaddr_get(port, &prefill_pkt.eth.s_addr);
#elif RTE_VER_YEAR == 22
    rte_ether_unformat_addr(SWITCH_MAC, &prefill_pkt.eth.dst_addr);
    rte_eth_macaddr_get(port, &prefill_pkt.eth.src_addr);
#else 
    #error Untested version
#endif
    htobe(&prefill_pkt.eth.ether_type, RTE_ETHER_TYPE_IPV4);

    // launch threads
    // rte_eal_mp_remote_launch()
    thread_ctx.resize(nworker);
    for(int lcore_id = 1; lcore_id < nthread; lcore_id ++) {// note: worker thread 0 run on lcore 1, main thread run on lcore 0
        int worker_id = lcore_id - 1;
        thread_ctx_t &ctx = thread_ctx[worker_id];
        ctx.port_id = port;
        ctx.worker_id = worker_id;
        ctx.tx_mbuf_pool = tx_mbuf_pool_list[worker_id];
        ctx.rx_mbuf_pool = rx_mbuf_pool_list[worker_id];
        ctx.qp = new std::remove_reference_t<decltype(*ctx.qp)>();

        htobe(&prefill_pkt.ip.type_of_service, (uint8_t)(worker_id << 2 | 0x1));
        ctx.window = new window_t(tx_mbuf_pool_list[worker_id], &prefill_pkt, sizeof(netccl_prefill_pkt));// init some packet headers
        
        ctx.process_buffer = new queue_process_buffer(ctx.port_id, ctx.worker_id, MAX_PKT_BURST, MAX_PKT_BURST);
        ctx.logger = spdlog::stdout_color_st(string("worker") + std::to_string(worker_id));
        // ctx.group_ctx is an empty map

        /* Simpler equivalent. 8< */
        retval = rte_eal_remote_launch(worker_loop, &ctx, lcore_id);
        assert(retval == 0);
        spdlog::info("lcore {} launched", lcore_id);
        // rte_eal_remote_launch(lcore_hello, NULL, lcore_id);
        /* >8 End of simpler equivalent. */
    }

    shm_init("netccl");

    load_balancer();

    // spdlog::info("exit");
    // retval = rte_eal_cleanup();
    // assert(retval == 0);
}

void shm_init(string _shm_name)
{
    shm_name = _shm_name;
    try {
        segment = new managed_shared_memory(open_only, shm_name.data());
    }
    catch(std::exception &e) {
        spdlog::error("shm_init: {}", e.what());
        // return;
        exit(1);
    }
    offset_ptr<shm_op_qp_t> *qp_ptr = segment->find<offset_ptr<shm_op_qp_t>>("queue_pair").first;// .second is the array length
    assert(qp_ptr != NULL && *qp_ptr != NULL);
    shm_qp = qp_ptr->get();
    assert(shm_qp != NULL);
}

int main(int argc, char **argv)
{
    // parse arg
    argparse::ArgumentParser program(argv[0], "", argparse::default_arguments::help);
    program.add_argument("<nthreads>").scan<'d', int>();
    try{
        program.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(1);
    }
    string ip = get_env_required("EPIC_BIND_ADDR");
    nthread = program.get<int>("<nthreads>");
    nworker = nthread - 1;

    // worker_init(nworker);
    dpdk_init(ip);
    return 0;
}






