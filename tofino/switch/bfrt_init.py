def NameSpace():
    import json 
    import sys
    import os

    program_name = bfrt.p4_programs_list[0]
    program = eval('bfrt.' + program_name)

    # NOTE: In bfrt, argv[0]=="/"
    config_dir = "."
    with open(config_dir + "/topo.json") as f: # symbolic link
        topo = json.load(f)

    port = topo["port"]
    MAC = topo["MAC"]
    IP = topo["IP"]
    switch_MAC = topo["switch_MAC"]
    switch_IP = topo["switch_IP"] # must be consistant with env NETCCL_SWITCH_ADDR
    recir_port = topo["recir_port"] # can add more

    # unicast
    for worker in range(len(port)):
        program.pipe.Ingress.Forward.l2_forward_table.add_with_forward(MAC[worker], port[worker])

    # port metadata
    for worker in range(len(port)):
        program.pipe.IngressParser.PORT_METADATA.add(port[worker], 0)
    for p in recir_port:
        program.pipe.IngressParser.PORT_METADATA.add(p, 1) # may use ether_type instead

    # bfrt.pre.node.add(123, 123, None, [port[2]]) # for debug, send to worker3
    # bfrt.pre.mgid.add(2, [123], [False], [0])

    # restore mac
    for worker in range(len(port)):
        program.pipe.Egress.restore_dmac_table.add_with_retore_dmac(IP[worker], MAC[worker])    

    program.pipe.Egress.restore_smac_table.add_with_retore_smac(switch_IP, switch_MAC)

    # dcqcn
    program.pipe.Egress.dcqcn.wred.add(0, 0, 125, 2500, 0.01)
    # 0 ~ 10KB, 0 
    # 10 ~ 200KB, 0 ~ 0.01
    # 200KB ~, 1

    for p in port:
        bfrt.port.port.add(p, 'BF_SPEED_100G', 'BF_FEC_TYP_RS', 4, True, 'PM_AN_FORCE_DISABLE')
    #        TX_PAUSE_FRAME_EN=1, RX_PAUSE_FRAME_EN=1, TX_PFC_EN_MAP=0xff, RX_PFC_EN_MAP=0xff)
    for p in recir_port:
        bfrt.port.port.add(p, 'BF_SPEED_100G', 'BF_FEC_TYP_NONE', 4, True, 'PM_AN_FORCE_DISABLE', 'BF_LPBK_MAC_NEAR')
    #        TX_PAUSE_FRAME_EN=1, RX_PAUSE_FRAME_EN=1, TX_PFC_EN_MAP=0xff, RX_PFC_EN_MAP=0xff)
    
    mcast_node_id = 2 ** 32 - 1
    mcast_node_list = []
    for p in port:
        bfrt.pre.node.add(mcast_node_id, 0, None, [p])
        mcast_node_list.append(mcast_node_id)
        mcast_node_id -= 1
    mgid = 2 ** 16 - 1
    bfrt.pre.mgid.add(mgid, mcast_node_list, [False] * len(mcast_node_list), [0] * len(mcast_node_list)) 
    program.pipe.Ingress.Forward.l2_forward_table.add_with_multicast(0xffffffffffff, mgid)

    # drop_probability = 0.0001
    # drop_probability = 0.001
    # drop_probability = 0
    # program.pipe.Egress.random_drop.drop_probability_table.set_default_with_get_probability(int(2**32 * drop_probability))

NameSpace()