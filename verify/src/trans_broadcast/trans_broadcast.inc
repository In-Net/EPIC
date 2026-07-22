configuration {
    // Property.
    TERMINATION_CHECK = FALSE;

    // Network nondeterminism.
    MAX_LOSS = 0;
    MAX_OUT_OF_ORDER = 0;
    MAX_DUPLICATION = 0;

    // System parameters.
    REQ_NUM = 1;
    WINDOW_SIZE = 1;
    FANIN = 2;

    // Packet types.
    RES = 1;
    ACK = 2;

    // Other constants.
    ROOT = 1;      // Root switch of reduction tree.
    COMM_ROOT = 7; // Root client of communication group.
}

topology {
    nodetype Client, Switch;
    node(Switch) r = 1, s12 = 2, s34 = 3;
    node(Client) c1 = 4, c2 = 5, c3 = 6, c4 = 7;
    link(unreliable) r -- s12, s34;
    link(unreliable) s12 -- c1, c2;
    link(unreliable) s34 -- c3, c4;
}

protocol {
    // Sets.
    const(all) REQ_SET = 0..(REQ_NUM - 1);
    const(all) AGG_NUM = 2 * WINDOW_SIZE;
    const(all) AGG_SET = 0..(AGG_NUM - 1);

    // Helper functions.
    fn SmallerOf(a, b) = IF a < b THEN a ELSE b;
    fn LargerOf(a, b) = IF a > b THEN a ELSE b;
    fn ParentOf(nid) = __next_hop[nid, COMM_ROOT];
    fn ChildrenOf(nid) = {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0};
    fn MakePkt(type, src, dst, psn) = [
        type |-> type,
        src |-> src,
        dst |-> dst,
        psn |-> psn
    ];
    fn MakePktWithValue(type, src, dst, psn, value) = [
        type |-> type,
        src |-> src,
        dst |-> dst,
        psn |-> psn,
        value |-> value
    ];
    fn Idx(psn) = psn % AGG_NUM;
    fn MinEEPsn(nid) = Min(Range(eepsn[nid]));
    fn CanTerminate(nid) = \A c \in NODE_SET : c > nid => __NodeTerminated(c);

    // Client requests.
    var(all) res = [i \in REQ_SET |-> RandomElement(0..100000)];
    var(all) requests = [i \in REQ_SET |->
        MakePktWithValue(RES, COMM_ROOT, __next_hop[COMM_ROOT, ROOT], i, res[i])];

    // Client states.
    var(all) base = -1;
    var(Client) res_epsn = 0;

    // Switch states.
    var(Switch) eepsn = [ch \in ChildrenOf(self) |-> 1];
    var(Switch) min_eepsn = 1;

    // Helper macros.

    macro __SendNewPkt(pkt) {
        // We did not use the deduplication mechanism provided by VeriNC.
        temp next = __next_hop[self, pkt.dst];
        if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], pkt)) {
            send(pkt);
        }
    }

    macro SendNewPkt(type, src, dst, psn) {
        __SendNewPkt(MakePkt(type, src, dst, psn));
    }
    macro SendNewPktWithValue(type, src, dst, psn, value) {
        __SendNewPkt(MakePktWithValue(type, src, dst, psn, value));
    }

    // Client handlers.

    macro ClientAck(pkt) {
        assert(self = COMM_ROOT, "ClientAck: non-root client receives ACK.");
        if (pkt.psn > base) {
            temp start_idx = base + WINDOW_SIZE;
            temp end_idx = SmallerOf(pkt.psn + WINDOW_SIZE, REQ_NUM);
            if (start_idx < end_idx) {
                unicast([i \in start_idx..(end_idx - 1) |-> requests[i]]);
            }
            base = pkt.psn;
            if (base = REQ_NUM) {
                exit();
            }
        }
    }

    macro ClientRes(pkt) {
        assert(self # COMM_ROOT, "ClientRes: root client receives RES.");
        assert(pkt.value = res[pkt.psn], "ClientRes: incorrect result.");
        if (pkt.psn < res_epsn) {
            SendNewPkt(ACK, self, pkt.src, res_epsn);
        }
        elif (pkt.psn = res_epsn) {
            res_epsn = res_epsn + 1;
            SendNewPkt(ACK, self, pkt.src, res_epsn);
        }
        else {
            // Possible design: reply with NAK for missing results.
            // Possible design: buffer results ahead.
            ;
        }
    }

    // Switch handlers.

    macro SwitchRes(pkt) {
        multicast(MakePktWithValue(RES, self, self, pkt.psn, pkt.value), ChildrenOf(self));
    }

    macro SwitchAck(pkt) {
        assert(pkt.src \in ChildrenOf(self), "SwitchAck: switch receives ACK from non-child.");
        temp idx = Idx(pkt.psn), ch = pkt.src;
        if (pkt.psn < eepsn[ch]) { // Behind.
            if (pkt.psn <= min_eepsn) {
                SendNewPkt(ACK, self, ParentOf(self), min_eepsn - 1);
            }
        }
        else { // Sync/Ahead.
            eepsn[ch] = pkt.psn + 1;
            // if (MinEEPsn(self) > min_eepsn) { // Old eepsn is used. Seems a bug in TLA+.
            if (\A c \in ChildrenOf(self) : eepsn[c] > min_eepsn) {
                min_eepsn = Min(Range(eepsn));
                SendNewPkt(ACK, self, ParentOf(self), min_eepsn - 1);
            }
        }
    }

    // Threads.

    thread(Client) ClientInitSend {
    L_ClientInitSend:
        if (self = COMM_ROOT) {
            temp end_idx = SmallerOf(WINDOW_SIZE, REQ_NUM);
            unicast([i \in 0..(end_idx - 1) |-> requests[i]]);
            base = 0;
        }
    }

    thread(Client) ClientRecv {
    L_ClientRecv:
        while (TRUE) {
            if (self # COMM_ROOT /\ CanTerminate(self)) {
                exit();
            }
            else {
                temp pkt = receive();
                if (pkt.type = RES) {
                    ClientRes(pkt);
                } elif (pkt.type = ACK) {
                    ClientAck(pkt);
                } else {
                    assert(FALSE, "ClientRecv: unexpected packet type");
                }
            }
        }
    }

    thread(Client) ClientRetx {
    L_ClientRetx:
        while (self = COMM_ROOT /\ base < REQ_NUM) {
            wait(base >= 0);
            wait(\A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>); // To reduce state space.
            temp end_idx = SmallerOf(base + WINDOW_SIZE, REQ_NUM);
            temp pkts = [i \in base..(end_idx - 1) |-> requests[i]];
            unicast(pkts);
        }
    }

    thread(Switch) SwitchRecv {
    L_SwitchRecv:
        while (~CanTerminate(self)) {
            temp pkt = receive();
            if (pkt.type = RES) {
                SwitchRes(pkt);
            } elif (pkt.type = ACK) {
                SwitchAck(pkt);
            } else {
                assert(FALSE, "SwitchRecv: unexpected packet type");
            }
        }
    }
}

property {
    NoDupPkt = \A nid \in NODE_SET : IsInjective(__net_buf[nid]);
    ConsistentEEPsn = \A sw \in SWITCH_SET : MinEEPsn(sw) = min_eepsn[sw];
}
