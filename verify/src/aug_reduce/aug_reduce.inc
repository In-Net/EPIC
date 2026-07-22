configuration {
    // Property.
    TERMINATION_CHECK = FALSE;

    // Network nondeterminism.
    MAX_LOSS = 0;
    MAX_OUT_OF_ORDER = 0;
    MAX_DUPLICATION = 0;

    // System parameters.
    AGG_NUM = 1;
    REQ_NUM = 1;
    WINDOW_SIZE = 1;
    FANIN = 2;

    // Packet types.
    REQ = 0;
    ACK = 2;
    NAK = 3;

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
    fn IsCompleted(nid, epsn, psn) = \A ch \in ChildrenOf(nid) : epsn[ch] > psn;
    fn MinEPsn(nid, epsn) = Min({epsn[ch] : ch \in ChildrenOf(nid)});
    fn SwitchCanTerminate(nid) = \A c \in NODE_SET : c > nid => __NodeTerminated(c);
    fn StrictRetxCondition(pkt) = \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>;
    fn PsnSetToIdxSet(range) = {Idx(psn) : psn \in range};
    fn RangeToSeq(start, stop) = [i \in 1..(stop - start + 1) |-> i + start - 1];

    // Client requests.
    var(all) req_val = [c \in CLIENT_SET, i \in REQ_SET |-> RandomElement(0..100000)];
    var(all) res = [i \in REQ_SET |-> SumSet({req_val[c, i] : c \in CLIENT_SET \ {COMM_ROOT}})];
    var(Client) requests = [i \in REQ_SET |->
        MakePktWithValue(REQ, self, ParentOf(self), i, req_val[self, i])];

    // Client states.
    var(Client) base = -1;
    var(all) res_epsn = 0;

    // Switch states.
    var(Switch) AggEPsn = [ch \in ChildrenOf(self) |-> 0];
    var(Switch) AggBuffer = [i \in AGG_SET |-> 0];
    var(Switch) AggPsnStart = 0;
    var(Switch) nak_sent = [ch \in ChildrenOf(self) |-> FALSE];
    var(Switch) recirc_pkts = <<>>;

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

    macro ClientNak(pkt) {
        assert(self # COMM_ROOT, "ClientNak: root client receives NAK.");
        if (pkt.psn >= base) {
            __SendNewPkt(requests[pkt.psn]);
        }
    }

    macro ClientReq(pkt) {
        assert(self = COMM_ROOT, "ClientReq: non-root client receives request.");
        assert(pkt.value = res[pkt.psn], "ClientReq: incorrect result.");
        if (pkt.psn = res_epsn) {
            res_epsn = res_epsn + 1;
            SendNewPkt(ACK, self, pkt.src, res_epsn);
            if (res_epsn = REQ_NUM) {
                exit();
            }
        }
    }

    // Helper macros used by switch handlers.

    macro Recirc(pkt) {
        recirc_pkts = Append(recirc_pkts, pkt);
    }
    macro RecircMultiple(pkts) {
        recirc_pkts = recirc_pkts \o pkts;
    }

    // Switch handlers.

    macro SwitchReq(pkt) {
        assert(pkt.src \in ChildrenOf(self), "SwitchReq: switch receives request from non-child.");
        temp idx = Idx(pkt.psn), ch = pkt.src;
        if (pkt.psn < AggEPsn[ch]) { // Behind.
            SendNewPkt(ACK, self, ch, AggEPsn[ch]);
        }
        elif (pkt.psn > AggEPsn[ch]) { // Ahead.
            if (~nak_sent[ch]) {
                SendNewPkt(NAK, self, ch, AggEPsn[ch]);
                nak_sent[ch] = TRUE;
            }
        }
        else { // Sync.
            nak_sent[ch] = FALSE;
            if (pkt.psn >= AggPsnStart + AGG_NUM) { // Buffer overflow.
                // Just drop it.
                ;
            }
            else {
                AggBuffer[idx] = AggBuffer[idx] + pkt.value;
                AggEPsn[ch] = AggEPsn[ch] + 1;
                SendNewPkt(ACK, self, ch, AggEPsn[ch]);
                if (IsCompleted(self, AggEPsn, pkt.psn)) { // Aggregation complete.
                    Recirc(MakePktWithValue(REQ, self, ParentOf(self), pkt.psn, AggBuffer[idx]));
                }
            }
        }
    }

    macro SwitchAck(pkt) {
        temp idx = Idx(pkt.psn);
        assert(pkt.src = ParentOf(self), "SwitchAck: switch receives ACK from non-parent.");
        assert(\A ch \in ChildrenOf(self) : AggEPsn[ch] >= pkt.psn,
            "SwitchAck: switch receives ACK for uncompleted aggregation.");
        assert(pkt.psn <= AggPsnStart + AGG_NUM, "SwitchAck: ACK for unstarted aggregation.");
        if (pkt.psn > AggPsnStart) {
            temp range = AggPsnStart..(pkt.psn - 1);
            AggBuffer = [i \in PsnSetToIdxSet(range) |-> 0] @@ AggBuffer;
            AggPsnStart = pkt.psn;
        }
    }

    macro SwitchNak(pkt) {
        temp idx = Idx(pkt.psn);
        assert(pkt.src = ParentOf(self), "SwitchNak: switch receives NAK from non-parent.");
        assert(pkt.psn = AggPsnStart, "SwitchNak: we currently do not consider out-of-order networks.");
        assert(IsCompleted(self, AggEPsn, pkt.psn), "SwitchNak: NAK for uncompleted aggregation.");
        temp max_psn = MinEPsn(self, AggEPsn) - 1;
        // VeriNC currently does not support higher-order functions.
        temp range_seq = RangeToSeq(AggPsnStart, max_psn);
        temp pkts = [i \in 1..Len(range_seq) |-> 
            MakePktWithValue(REQ, self, ParentOf(self), range_seq[i], AggBuffer[Idx(range_seq[i])])];
        RecircMultiple(pkts);
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
            temp pkt = receive();
            if (pkt.type = REQ) {
                ClientReq(pkt);
            } elif (pkt.type = ACK) {
                ClientAck(pkt);
            } elif (pkt.type = NAK) {
                ClientNak(pkt);
            } else {
                assert(FALSE, "ClientRecv: unexpected packet type");
            }
        }
    }

    thread(Client) ClientRetx {
    L_ClientRetx:
        while (self # COMM_ROOT /\ base < REQ_NUM) {
            wait(base >= 0);
            wait(StrictRetxCondition(0));
            temp end_idx = SmallerOf(base + WINDOW_SIZE, REQ_NUM);
            unicast([i \in base..(end_idx - 1) |-> requests[i]]);
        }
    }

    thread(Switch) SwitchRecv {
    L_SwitchRecv:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts = <<>>);
            temp pkt = receive();
            if (pkt.type = REQ) {
                SwitchReq(pkt);
            } elif (pkt.type = ACK) {
                SwitchAck(pkt);
            } elif (pkt.type = NAK) {
                SwitchNak(pkt);
            } else {
                assert(FALSE, "SwitchRecv: unexpected packet type");
            }
        }
    }

    thread(Switch) SwitchRecirc {
    L_SwitchRecirc:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts # <<>>);
            __SendNewPkt(Head(recirc_pkts));
            recirc_pkts = Tail(recirc_pkts);
        }
    }

    thread(Switch) SwitchRetxReq {
    L_SwitchRetxReq:
        while (~SwitchCanTerminate(self)) {
            wait(recirc_pkts = <<>>);
            wait(StrictRetxCondition(0));
            wait(IsCompleted(self, AggEPsn, AggPsnStart));
            temp max_psn = MinEPsn(self, AggEPsn) - 1;
            temp range_seq = RangeToSeq(AggPsnStart, max_psn);
            temp pkts = [i \in 1..Len(range_seq) |->
                MakePktWithValue(REQ, self, ParentOf(self), range_seq[i], AggBuffer[Idx(range_seq[i])])];
            RecircMultiple(pkts);
        }
    }
}

property {
    AggEPsnInRange = \A nid \in SWITCH_SET : \A ch \in ChildrenOf(nid) :
        AggEPsn[nid][ch] >= AggPsnStart[nid] /\ AggEPsn[nid][ch] <= AggPsnStart[nid] + AGG_NUM;
}
