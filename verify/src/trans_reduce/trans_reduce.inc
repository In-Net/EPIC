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
    REQ = 0;
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
    fn Dual(idx) = IF idx < WINDOW_SIZE THEN idx + WINDOW_SIZE ELSE idx - WINDOW_SIZE;
    fn IsCompleted(nid, state, psn) = \A i \in ChildrenOf(nid) : state[Idx(psn), i] = 1;
    fn SwitchCanTerminate(nid) = \A c \in NODE_SET : c > nid => __NodeTerminated(c);

    // Client requests.
    var(all) req_val = [c \in CLIENT_SET, i \in REQ_SET |-> RandomElement(0..100000)];
    var(all) res = [i \in REQ_SET |-> SumSet({req_val[c, i] : c \in CLIENT_SET \ {COMM_ROOT}})];
    var(Client) requests = [i \in REQ_SET |->
        MakePktWithValue(REQ, self, ParentOf(self), i, req_val[self, i])];

    // Client states.
    var(Client) base = -1;
    var(Client) res_epsn = 0;

    // Switch states.
    var(Switch) ArrState = [i \in AGG_SET, ch \in ChildrenOf(self) |-> 0];
    var(Switch) AggBuffer = [i \in AGG_SET |-> 0];
    var(Switch) ePSN = [ch \in ChildrenOf(self) |-> 0];

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

    macro ClientReq(pkt) {
        assert(self = COMM_ROOT, "ClientReq: non-root client receives request.");
        assert(pkt.value = res[pkt.psn], "ClientReq: incorrect result.");
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

    macro ClientAck(pkt) {
        assert(self # COMM_ROOT, "ClientAck: root client receives ACK.");
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

    // Switch handlers.

    macro SwitchReq(pkt) {
        assert(pkt.src \in ChildrenOf(self), "SwitchReq: switch receives request from non-child.");
        temp idx = Idx(pkt.psn), ch = pkt.src;
        if (pkt.psn < ePSN[ch]) { // Behind.
            if (IsCompleted(self, ArrState, pkt.psn)) {
                SendNewPktWithValue(REQ, self, ParentOf(self), pkt.psn, AggBuffer[idx]);
            }
            // Possible design: trigger reduce progress analysis.
        }
        elif (pkt.psn > ePSN[ch]) { // Ahead.
            // Do nothing. No buffering is consistent with GBN.
            ;
        }
        else { // Sync.
            assert(ArrState[idx, ch] = 0, "SwitchReq: switch receives sync but duplicated REQ.");
            ePSN[ch] = ePSN[ch] + 1;
            ArrState[idx, ch] = 1, ArrState[Dual(idx), ch] = 0;
            AggBuffer[idx] = AggBuffer[idx] + pkt.value, AggBuffer[Dual(idx)] = 0;
            if (IsCompleted(self, ArrState, pkt.psn)) { // Aggregation complete.
                SendNewPktWithValue(REQ, self, ParentOf(self), pkt.psn, AggBuffer[idx]);
            }
        }
    }

    macro SwitchAck(pkt) {
        multicast(MakePkt(ACK, self, self, pkt.psn), ChildrenOf(self));
    }

    // Threads.

    thread(Client) ClientInitSend {
    L_ClientInitSend:
        if (self # COMM_ROOT) {
            temp end_idx = SmallerOf(WINDOW_SIZE, REQ_NUM);
            unicast([i \in 0..(end_idx - 1) |-> requests[i]]);
            base = 0;
        }
    }

    thread(Client) ClientRecv {
    L_ClientRecv:
        while (TRUE) {
            if (self = COMM_ROOT /\ \A c \in CLIENT_SET : c # self => __NodeTerminated(c)) {
                exit();
            }
            else {
                temp pkt = receive();
                if (pkt.type = REQ) {
                    ClientReq(pkt);
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
        while (self # COMM_ROOT /\ base < REQ_NUM) {
            wait(base >= 0);
            wait(\A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>); // To reduce state space.
            temp end_idx = SmallerOf(base + WINDOW_SIZE, REQ_NUM);
            temp pkts = [i \in base..(end_idx - 1) |-> requests[i]];
            unicast(pkts);
        }
    }

    thread(Switch) SwitchRecv {
    L_SwitchRecv:
        while (~SwitchCanTerminate(self)) {
            temp pkt = receive();
            if (pkt.type = REQ) {
                SwitchReq(pkt);
            } elif (pkt.type = ACK) {
                SwitchAck(pkt);
            } else {
                assert(FALSE, "SwitchRecv: unexpected packet type");
            }
        }
    }
}
