---- MODULE aug_reduce ----

EXTENDS Integers, Sequences, FiniteSets, TLC, Bitwise, FiniteSetsExt, SequencesExt, Functions

CONSTANTS
  TERMINATION_CHECK,
  MAX_LOSS,
  MAX_OUT_OF_ORDER,
  MAX_DUPLICATION,
  AGG_NUM,
  REQ_NUM,
  WINDOW_SIZE,
  FANIN,
  REQ,
  ACK,
  NAK,
  ROOT,
  COMM_ROOT,
  r,
  s12,
  s34,
  c1,
  c2,
  c3,
  c4,
  null

(* --fair algorithm main {
  variables
    __net_buf = [__n \in NODE_SET |-> <<>>];
    __max_loss = MAX_LOSS;
    __max_out_of_order = MAX_OUT_OF_ORDER;
    __max_duplication = MAX_DUPLICATION;
    req_val = [c \in CLIENT_SET, i \in REQ_SET |-> RandomElement(0 .. 100000)];
    res = [i \in REQ_SET |-> SumSet({req_val[c, i] : c \in CLIENT_SET \ {COMM_ROOT}})];
    res_epsn = 0;
    __active_threads = [__n \in SWITCH_SET |-> 3] @@ [__n \in CLIENT_SET |-> 3];
    AggEPsn = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> 0])];
    AggBuffer = [__n \in SWITCH_SET |-> ([i \in AGG_SET |-> 0])];
    AggPsnStart = [__n \in SWITCH_SET |-> (0)];
    nak_sent = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> FALSE])];
    recirc_pkts = [__n \in SWITCH_SET |-> (<<>>)];
    requests = [__n \in CLIENT_SET |-> ([i \in REQ_SET |-> MakePktWithValue(REQ, __n, ParentOf(__n), i, req_val[__n, i])])];
    base = [__n \in CLIENT_SET |-> (- 1)];

  define {
    SWITCH_SET == {r, s12, s34}
    SWITCH_NUM == Cardinality(SWITCH_SET)
    CLIENT_SET == {c1, c2, c3, c4}
    CLIENT_NUM == Cardinality(CLIENT_SET)
    NODE_SET == {r, s12, s34, c1, c2, c3, c4}
    NODE_NUM == Cardinality(NODE_SET)
    __links == r :> {s34, s12} @@ s12 :> {c2, c1, r} @@ s34 :> {c4, c3, r} @@ c1 :> {s12} @@ c2 :> {s12} @@ c3 :> {s34} @@ c4 :> {s34}
    __reliable_links == {<<c4, c4>>, <<c3, c3>>, <<c2, c2>>, <<c1, c1>>, <<s34, s34>>, <<s12, s12>>, <<r, r>>}
    __next_hop == 
          <<r, r>> :> r @@ <<r, s12>> :> s12 @@ <<r, s34>> :> s34
       @@ <<r, c1>> :> s12 @@ <<r, c2>> :> s12 @@ <<r, c3>> :> s34
       @@ <<r, c4>> :> s34 @@ <<s12, r>> :> r @@ <<s12, s12>> :> s12
       @@ <<s12, s34>> :> r @@ <<s12, c1>> :> c1 @@ <<s12, c2>> :> c2
       @@ <<s12, c3>> :> r @@ <<s12, c4>> :> r @@ <<s34, r>> :> r
       @@ <<s34, s12>> :> r @@ <<s34, s34>> :> s34 @@ <<s34, c1>> :> r
       @@ <<s34, c2>> :> r @@ <<s34, c3>> :> c3 @@ <<s34, c4>> :> c4
       @@ <<c1, r>> :> s12 @@ <<c1, s12>> :> s12 @@ <<c1, s34>> :> s12
       @@ <<c1, c1>> :> c1 @@ <<c1, c2>> :> s12 @@ <<c1, c3>> :> s12
       @@ <<c1, c4>> :> s12 @@ <<c2, r>> :> s12 @@ <<c2, s12>> :> s12
       @@ <<c2, s34>> :> s12 @@ <<c2, c1>> :> s12 @@ <<c2, c2>> :> c2
       @@ <<c2, c3>> :> s12 @@ <<c2, c4>> :> s12 @@ <<c3, r>> :> s34
       @@ <<c3, s12>> :> s34 @@ <<c3, s34>> :> s34 @@ <<c3, c1>> :> s34
       @@ <<c3, c2>> :> s34 @@ <<c3, c3>> :> c3 @@ <<c3, c4>> :> s34
       @@ <<c4, r>> :> s34 @@ <<c4, s12>> :> s34 @@ <<c4, s34>> :> s34
       @@ <<c4, c1>> :> s34 @@ <<c4, c2>> :> s34 @@ <<c4, c3>> :> s34
       @@ <<c4, c4>> :> c4
    REQ_SET == 0 .. (REQ_NUM - 1)
    AGG_SET == 0 .. (AGG_NUM - 1)

    __IsReliableLink(__src, __dst) == <<__src, __dst>> \in __reliable_links
    __IsUnreliableLink(__src, __dst) == <<__src, __dst>> \notin __reliable_links
    __MinPsnElem(__S) == CHOOSE __x \in __S : \A __y \in __S : __x.psn <= __y.psn
    __Set2OrderedSeq(__S) == 
      LET
        RECURSIVE F(_, _)
        F(__res, __SS) == IF __SS = {}
          THEN __res
          ELSE LET __x == __MinPsnElem(__SS) IN F(Append(__res, __x), __SS \ {__x})
      IN F(<<>>, __S)
    __OutOfOrderRange(__seq) == 1..Len(__seq)
    __InsertAtEnd(__seq, __i, __elem) == InsertAt(__seq, Len(__seq) - __i + 1, __elem)
    __Receive(__n) == Head(__net_buf[__n])
    __Node(__thread) == Head(__thread)
    __PosCount(__f) == Cardinality({__x \in DOMAIN __f : __f[__x] > 0})
    __AllPossibleLoss(__S) == {__s \in SUBSET __S : Cardinality(__s) <= __max_loss}
    __AllPossibleOutOfOrder(__S) == 
      LET
        __max_range == IF __S = {} THEN {} ELSE 0..Max({Len(__net_buf[__i]) : __i \in __S})
        __ooo_set == [__S -> __max_range]
        __ooo_set_possible == {__i \in __ooo_set : 
          /\ (\A __j \in DOMAIN __i : __i[__j] \in __OutOfOrderRange(__net_buf[__j]) \cup {0})
          /\ __PosCount(__i) <= __max_out_of_order
        }
      IN __ooo_set_possible
    __AllPossibleSeq(__S) == {__seq \in [1..Cardinality(__S) -> __S] : IsInjective(__seq)}
    __NodeTerminated(__n) == __active_threads[__n] <= 0
    SmallerOf(a, b) == IF a < b THEN a ELSE b
    LargerOf(a, b) == IF a > b THEN a ELSE b
    ParentOf(nid) == __next_hop[nid, COMM_ROOT]
    ChildrenOf(nid) == {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0}
    MakePkt(type, src, dst, psn) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn]
    MakePktWithValue(type, src, dst, psn, value) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn, value |-> value]
    Idx(psn) == psn % AGG_NUM
    IsCompleted(nid, epsn, psn) == \A ch \in ChildrenOf(nid) : epsn[ch] > psn
    MinEPsn(nid, epsn) == Min({epsn[ch] : ch \in ChildrenOf(nid)})
    SwitchCanTerminate(nid) == \A c \in NODE_SET : c > nid => __NodeTerminated(c)
    StrictRetxCondition(pkt) == \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>
    PsnSetToIdxSet(range) == {Idx(psn) : psn \in range}
    RangeToSeq(start, stop) == [i \in 1 .. (stop - start + 1) |-> i + start - 1]
  }


  macro __Assert(__cond, __msg) {
    with (__b = Assert(__cond, __msg)) {
      assert __b;
    }
  }

  macro __Drop() {
    __Assert(__net_buf[__Node(self)] # <<>>, "Drop: empty buffer");
    __net_buf[__Node(self)] := Tail(@);
  }

  macro __Send_NoDup(__pkt) {
    with (__h = __next_hop[__Node(self), __pkt.dst]) {
      either {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_out_of_order > 0;
        await __OutOfOrderRange(__net_buf[__h]) # {};
        with (__pos \in __OutOfOrderRange(__net_buf[__h])) {
          __net_buf[__h] := __InsertAtEnd(@, __pos, __pkt);
        };
        __max_out_of_order := __max_out_of_order - 1;
      }
      or {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_loss > 0;
        __max_loss := __max_loss - 1;
      }
      or {
        __net_buf[__h] := Append(@, __pkt);
      }
    }
  }

  macro __Send(__pkt) {
    with (
      has_dup = 
        /\ "id" \in DOMAIN __pkt
        /\ \E __n \in NODE_SET : \E __i \in DOMAIN __net_buf[__n] :
          "id" \in DOMAIN __net_buf[__n][__i] /\ __net_buf[__n][__i].id = __pkt.id
    ) {
      if (~has_dup) {
        __Send_NoDup(__pkt);
      }
      else {
        either {
          await __max_duplication > 0;
          __max_duplication := __max_duplication - 1;
          __Send_NoDup(__pkt);
        }
        or {
          skip;
        }
      }
    }
  }

  macro __DropSend(__pkt) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropSend: empty buffer");
    with (__s = __Node(self), __h = __next_hop[__s, __pkt.dst]) {
      either {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_out_of_order > 0;
        await __OutOfOrderRange(__net_buf[__h]) # {};
        with (__pos \in __OutOfOrderRange(__net_buf[__h])) {
          if (__s = __h) {
            __net_buf[__h] := Tail(__InsertAtEnd(@, __pos, __pkt));
          } else {
            __net_buf[__h] := __InsertAtEnd(@, __pos, __pkt) ||
            __net_buf[__s] := Tail(@);
          }
        };
        __max_out_of_order := __max_out_of_order - 1;
      }
      or {
        await __IsUnreliableLink(__Node(self), __h);
        await __max_loss > 0;
        __max_loss := __max_loss - 1;
        __net_buf[__s] := Tail(@);
      }
      or {
        if (__s = __h) {
          __net_buf[__h] := Tail(Append(@, __pkt));
        } else {
          __net_buf[__h] := Append(@, __pkt) ||
          __net_buf[__s] := Tail(@);
        }
      }
    }
  }

  macro __Unicast(__pkts) {
    with (
      __keys = DOMAIN __pkts,
      __dst = __pkts[CHOOSE __i \in __keys : TRUE].dst,
      __h = __next_hop[__Node(self), __dst],
      __pkts_ordered = __Set2OrderedSeq({__pkts[__i] : __i \in __keys})
    ) {
      __Assert(Cardinality(__keys) > 0, "Unicast: empty packets");
      __Assert(
        \A __i \in __keys : __pkts[__i].dst = __dst,
        "Unicast: different destinations"
      );
      if (__IsReliableLink(__Node(self), __h)) {
        __net_buf[__h] := @ \o __pkts_ordered;
      }
      else {
        with (
          __loss \in __AllPossibleLoss(__keys),
          __unlost_pkts = {__pkts[__i] : __i \in __keys \ __loss},
          __ordered = __Set2OrderedSeq(__unlost_pkts)
        ) {
          __max_loss := __max_loss - Cardinality(__loss);
          either {
            await __max_out_of_order > 0;
            await Cardinality(__unlost_pkts) >= 2;
            with (__ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}) {
              __max_out_of_order := __max_out_of_order - 1;
              __net_buf[__h] := @ \o __ooo;
            }
          }
          or {
            __net_buf[__h] := @ \o __ordered;
          }
        }
      }
    }
  }

  macro __DropUnicast(__pkts) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropUnicast: empty buffer");
    with (
      __keys = DOMAIN __pkts,
      __dst = __pkts[CHOOSE __i \in __keys : TRUE].dst,
      __s = __Node(self),
      __h = __next_hop[__s, __dst],
      __pkts_ordered = __Set2OrderedSeq({__pkts[__i] : __i \in __keys})
    ) {
      __Assert(Cardinality(__keys) > 0, "DropUnicast: empty packets");
      __Assert(
        \A __i \in __keys : __pkts[__i].dst = __dst,
        "DropUnicast: different destinations"
      );
      if (__IsReliableLink(__s, __h)) {
        if (__s = __h) {
          __net_buf[__h] := Tail(@ \o __pkts_ordered);
        } else {
          __net_buf[__h] := @ \o __pkts_ordered ||
          __net_buf[__s] := Tail(@);
        }
      }
      else {
        with (
          __loss \in __AllPossibleLoss(__keys),
          __unlost_pkts = {__pkts[__i] : __i \in __keys \ __loss},
          __ordered = __Set2OrderedSeq(__unlost_pkts)
        ) {
          __max_loss := __max_loss - Cardinality(__loss);
          either {
            await __max_out_of_order > 0;
            await Cardinality(__unlost_pkts) >= 2;
            with (__ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}) {
              __max_out_of_order := __max_out_of_order - 1;
              if (__s = __h) {
                __net_buf[__h] := Tail(@ \o __ooo);
              } else {
                __net_buf[__h] := @ \o __ooo ||
                __net_buf[__s] := Tail(@);
              }
            }
          }
          or {
            if (__s = __h) {
              __net_buf[__h] := Tail(@ \o __ordered);
            } else {
              __net_buf[__h] := @ \o __ordered ||
              __net_buf[__s] := Tail(@);
            }
          }
        }
      }
    }
  }

  macro __Multicast(__pkt, __dsts) {
    __Assert(__dsts \subseteq __links[__Node(self)], "Multicast: invalid destinations");
    __Assert(__Node(self) \notin __dsts, "DropMulticast: self in destinations");
    with (
      __pkts = [__dst \in __dsts |-> "dst" :> __dst @@ __pkt],
      __possible_losses_1 = __AllPossibleLoss(__dsts),
      __possible_losses_2 = {__l \in __possible_losses_1 :
        \A __d \in __l : __IsUnreliableLink(__Node(self), __d)},
      __loss \in __possible_losses_2,
      __unlost_dsts = __dsts \ __loss,
      __possible_ooo_1 = __AllPossibleOutOfOrder(__unlost_dsts),
      __possible_ooo_2 = {__o \in __possible_ooo_1 : \A __d \in DOMAIN __o : __o[__d] > 0 => __IsUnreliableLink(__Node(self), __d)},
      __ooo \in __possible_ooo_2
    ) {
      __net_buf := [__n \in NODE_SET |->
        CASE __n \in __unlost_dsts -> __InsertAtEnd(__net_buf[__n], __ooo[__n], __pkts[__n])
        [] OTHER -> __net_buf[__n]
      ];
      __max_loss := __max_loss - Cardinality(__loss);
      __max_out_of_order := __max_out_of_order - __PosCount(__ooo);
    }
  }

  macro __DropMulticast(__pkt, __dsts) {
    __Assert(__net_buf[__Node(self)] # <<>>, "DropMulticast: empty buffer");
    __Assert(__dsts \subseteq __links[__Node(self)], "DropMulticast: invalid destinations");
    __Assert(__Node(self) \notin __dsts, "DropMulticast: self in destinations");
    with (
      __pkts = [__dst \in __dsts |-> "dst" :> __dst @@ __pkt],
      __possible_losses_1 = __AllPossibleLoss(__dsts),
      __possible_losses_2 = {__l \in __possible_losses_1 :
        \A __d \in __l : __IsUnreliableLink(__Node(self), __d)},
      __loss \in __possible_losses_2,
      __unlost_dsts = __dsts \ __loss,
      __possible_ooo_1 = __AllPossibleOutOfOrder(__unlost_dsts),
      __possible_ooo_2 = {__o \in __possible_ooo_1 :
        \A __d \in DOMAIN __o : __o[__d] > 0 => __IsUnreliableLink(__Node(self), __d)},
      __ooo \in __possible_ooo_2
    ) {
      __net_buf := [__n \in NODE_SET |->
        CASE __n \in __unlost_dsts -> __InsertAtEnd(__net_buf[__n], __ooo[__n], __pkts[__n])
        [] __n = __Node(self) -> Tail(__net_buf[__n])
        [] OTHER -> __net_buf[__n]
      ];
      __max_loss := __max_loss - Cardinality(__loss);
      __max_out_of_order := __max_out_of_order - __PosCount(__ooo);
    }
  }

  macro __Wait(__cond) {
    await __cond;
  }

  macro __Exit() {
    __active_threads[__Node(self)] := 0;
    goto Done;
  }

  macro __Print(__x) {
    print __x;
  }

  macro __CheckCacheConsistency(__pkt, __is_start) {
    skip;
  }


  fair+ process (ClientInitSend \in (CLIENT_SET \X {"ClientInitSend"})) {
  L_ClientInitSend:
    if (__active_threads[__Node(self)] <= 0) { goto Done; };
    else {
      if (__Node(self) # COMM_ROOT) {
        with (
          end_idx = SmallerOf(WINDOW_SIZE, REQ_NUM),
        ) {
          __Unicast([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]]);
          base[__Node(self)] := 0;
        }
      };
    };
\*   __L_ClientInitSend_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (ClientRecv \in (CLIENT_SET \X {"ClientRecv"})) {
  L_ClientRecv:
    while(TRUE) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          pkt = __Receive(__Node(self)),
        ) {
          __Wait(__net_buf[__Node(self)] # <<>>);
          if (pkt.type = REQ) {
            __Assert(__Node(self) = COMM_ROOT, "ClientReq: non-root client receives request.");
            __Assert((pkt).value = res[(pkt).psn], "ClientReq: incorrect result.");
            if ((pkt).psn = res_epsn) {
              with (
                next = __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn))).dst],
              ) {
                res_epsn := res_epsn + 1;
                if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn))))) {
                  __DropSend((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn))));
                };
                else {
                  __Drop();
                };
                if (res_epsn = REQ_NUM) {
                  __Exit();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else if (pkt.type = ACK) {
            __Assert(__Node(self) # COMM_ROOT, "ClientAck: root client receives ACK.");
            if ((pkt).psn > base[__Node(self)]) {
              with (
                start_idx = base[__Node(self)] + WINDOW_SIZE,
                end_idx = SmallerOf((pkt).psn + WINDOW_SIZE, REQ_NUM),
              ) {
                if (start_idx < end_idx) {
                  __DropUnicast([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]]);
                };
                else {
                  __Drop();
                };
                base[__Node(self)] := (pkt).psn;
                if (base[__Node(self)] = REQ_NUM) {
                  __Exit();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else if (pkt.type = NAK) {
            __Assert(__Node(self) # COMM_ROOT, "ClientNak: root client receives NAK.");
            if ((pkt).psn >= base[__Node(self)]) {
              with (
                next = __next_hop[__Node(self), (requests[__Node(self)][(pkt).psn]).dst],
              ) {
                if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (requests[__Node(self)][(pkt).psn]))) {
                  __DropSend((requests[__Node(self)][(pkt).psn]));
                };
                else {
                  __Drop();
                };
              }
            };
            else {
              __Drop();
            };
          };
          else {
            __Assert(FALSE, "ClientRecv: unexpected packet type");
            __Drop();
          };
        }
      };
    };
\*   __L_ClientRecv_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (ClientRetx \in (CLIENT_SET \X {"ClientRetx"})) {
  L_ClientRetx:
    while(__Node(self) # COMM_ROOT /\ base[__Node(self)] < REQ_NUM) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          end_idx = SmallerOf(base[__Node(self)] + WINDOW_SIZE, REQ_NUM),
        ) {
          __Wait(__net_buf[__Node(self)] = <<>>);
          __Wait(base[__Node(self)] >= 0);
          __Wait(StrictRetxCondition(0));
          __Unicast([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]]);
        }
      };
    };
\*   __L_ClientRetx_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (SwitchRecv \in (SWITCH_SET \X {"SwitchRecv"})) {
  L_SwitchRecv:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          pkt = __Receive(__Node(self)),
        ) {
          __Wait(recirc_pkts[__Node(self)] = <<>>);
          __Wait(__net_buf[__Node(self)] # <<>>);
          if (pkt.type = REQ) {
            with (
              idx = Idx((pkt).psn),
              ch = (pkt).src,
            ) {
              __Assert((pkt).src \in ChildrenOf(__Node(self)), "SwitchReq: switch receives request from non-child.");
              if ((pkt).psn < AggEPsn[__Node(self)][ch]) {
                with (
                  next = __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst],
                ) {
                  if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))) {
                    __DropSend((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))));
                  };
                  else {
                    __Drop();
                  };
                }
              };
              else if ((pkt).psn > AggEPsn[__Node(self)][ch]) {
                if (~nak_sent[__Node(self)][ch]) {
                  with (
                    next = __next_hop[__Node(self), (MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst],
                  ) {
                    if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))) {
                      __DropSend((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))));
                    };
                    else {
                      __Drop();
                    };
                    nak_sent[__Node(self)][ch] := TRUE;
                  }
                };
                else {
                  __Drop();
                };
              };
              else {
                nak_sent[__Node(self)][ch] := FALSE;
                if ((pkt).psn >= AggPsnStart[__Node(self)] + AGG_NUM) {
                  skip;
                  __Drop();
                };
                else {
                  with (
                    next = __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst],
                  ) {
                    AggBuffer[__Node(self)][idx] := AggBuffer[__Node(self)][idx] + (pkt).value;
                    AggEPsn[__Node(self)][ch] := AggEPsn[__Node(self)][ch] + 1;
                    if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))) {
                      __DropSend((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))));
                    };
                    else {
                      __Drop();
                    };
                    if (IsCompleted(__Node(self), AggEPsn[__Node(self)], (pkt).psn)) {
                      recirc_pkts[__Node(self)] := Append(recirc_pkts[__Node(self)], (MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), (pkt).psn, AggBuffer[__Node(self)][idx])));
                    };
                  }
                };
              };
            }
          };
          else if (pkt.type = ACK) {
            with (
              idx = Idx((pkt).psn),
            ) {
              __Assert((pkt).src = ParentOf(__Node(self)), "SwitchAck: switch receives ACK from non-parent.");
              __Assert(\A ch \in ChildrenOf(__Node(self)) : AggEPsn[__Node(self)][ch] >= (pkt).psn, "SwitchAck: switch receives ACK for uncompleted aggregation.");
              __Assert((pkt).psn <= AggPsnStart[__Node(self)] + AGG_NUM, "SwitchAck: ACK for unstarted aggregation.");
              if ((pkt).psn > AggPsnStart[__Node(self)]) {
                with (
                  range = AggPsnStart[__Node(self)] .. ((pkt).psn - 1),
                ) {
                  AggBuffer[__Node(self)] := [i \in PsnSetToIdxSet(range) |-> 0] @@ AggBuffer[__Node(self)];
                  AggPsnStart[__Node(self)] := (pkt).psn;
                }
              };
              __Drop();
            }
          };
          else if (pkt.type = NAK) {
            with (
              idx = Idx((pkt).psn),
              max_psn = MinEPsn(__Node(self), AggEPsn[__Node(self)]) - 1,
              range_seq = RangeToSeq(AggPsnStart[__Node(self)], max_psn),
              pkts = [i \in 1 .. Len(range_seq) |-> MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), range_seq[i], AggBuffer[__Node(self)][Idx(range_seq[i])])],
            ) {
              __Assert((pkt).src = ParentOf(__Node(self)), "SwitchNak: switch receives NAK from non-parent.");
              __Assert((pkt).psn = AggPsnStart[__Node(self)], "SwitchNak: we currently do not consider out-of-order networks.");
              __Assert(IsCompleted(__Node(self), AggEPsn[__Node(self)], (pkt).psn), "SwitchNak: NAK for uncompleted aggregation.");
              recirc_pkts[__Node(self)] := recirc_pkts[__Node(self)] \o (pkts);
              __Drop();
            }
          };
          else {
            __Assert(FALSE, "SwitchRecv: unexpected packet type");
            __Drop();
          };
        }
      };
    };
\*   __L_SwitchRecv_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
    __Exit();
  }

  fair+ process (SwitchRecirc \in (SWITCH_SET \X {"SwitchRecirc"})) {
  L_SwitchRecirc:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          next = __next_hop[__Node(self), (Head(recirc_pkts[__Node(self)])).dst],
        ) {
          __Wait(recirc_pkts[__Node(self)] # <<>>);
          if (~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (Head(recirc_pkts[__Node(self)])))) {
            __Send((Head(recirc_pkts[__Node(self)])));
          };
          recirc_pkts[__Node(self)] := Tail(recirc_pkts[__Node(self)]);
        }
      };
    };
\*   __L_SwitchRecirc_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }

  fair+ process (SwitchRetxReq \in (SWITCH_SET \X {"SwitchRetxReq"})) {
  L_SwitchRetxReq:
    while(~SwitchCanTerminate(__Node(self))) {
      if (__active_threads[__Node(self)] <= 0) { goto Done; };
      else {
        with (
          max_psn = MinEPsn(__Node(self), AggEPsn[__Node(self)]) - 1,
          range_seq = RangeToSeq(AggPsnStart[__Node(self)], max_psn),
          pkts = [i \in 1 .. Len(range_seq) |-> MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), range_seq[i], AggBuffer[__Node(self)][Idx(range_seq[i])])],
        ) {
          __Wait(recirc_pkts[__Node(self)] = <<>>);
          __Wait(StrictRetxCondition(0));
          __Wait(IsCompleted(__Node(self), AggEPsn[__Node(self)], AggPsnStart[__Node(self)]));
          recirc_pkts[__Node(self)] := recirc_pkts[__Node(self)] \o (pkts);
        }
      };
    };
\*   __L_SwitchRetxReq_End:
\*     if (__active_threads[__Node(self)] > 0) {
\*       __active_threads[__Node(self)] := @ - 1;
\*     };
  }
} *)
\* BEGIN TRANSLATION (chksum(pcal) = "7c28162a" /\ chksum(tla) = "fafeecdf")
VARIABLES pc, __net_buf, __max_loss, __max_out_of_order, __max_duplication, 
          req_val, res, res_epsn, __active_threads, AggEPsn, AggBuffer, 
          AggPsnStart, nak_sent, recirc_pkts, requests, base

(* define statement *)
SWITCH_SET == {r, s12, s34}
SWITCH_NUM == Cardinality(SWITCH_SET)
CLIENT_SET == {c1, c2, c3, c4}
CLIENT_NUM == Cardinality(CLIENT_SET)
NODE_SET == {r, s12, s34, c1, c2, c3, c4}
NODE_NUM == Cardinality(NODE_SET)
__links == r :> {s34, s12} @@ s12 :> {c2, c1, r} @@ s34 :> {c4, c3, r} @@ c1 :> {s12} @@ c2 :> {s12} @@ c3 :> {s34} @@ c4 :> {s34}
__reliable_links == {<<c4, c4>>, <<c3, c3>>, <<c2, c2>>, <<c1, c1>>, <<s34, s34>>, <<s12, s12>>, <<r, r>>}
__next_hop ==
      <<r, r>> :> r @@ <<r, s12>> :> s12 @@ <<r, s34>> :> s34
   @@ <<r, c1>> :> s12 @@ <<r, c2>> :> s12 @@ <<r, c3>> :> s34
   @@ <<r, c4>> :> s34 @@ <<s12, r>> :> r @@ <<s12, s12>> :> s12
   @@ <<s12, s34>> :> r @@ <<s12, c1>> :> c1 @@ <<s12, c2>> :> c2
   @@ <<s12, c3>> :> r @@ <<s12, c4>> :> r @@ <<s34, r>> :> r
   @@ <<s34, s12>> :> r @@ <<s34, s34>> :> s34 @@ <<s34, c1>> :> r
   @@ <<s34, c2>> :> r @@ <<s34, c3>> :> c3 @@ <<s34, c4>> :> c4
   @@ <<c1, r>> :> s12 @@ <<c1, s12>> :> s12 @@ <<c1, s34>> :> s12
   @@ <<c1, c1>> :> c1 @@ <<c1, c2>> :> s12 @@ <<c1, c3>> :> s12
   @@ <<c1, c4>> :> s12 @@ <<c2, r>> :> s12 @@ <<c2, s12>> :> s12
   @@ <<c2, s34>> :> s12 @@ <<c2, c1>> :> s12 @@ <<c2, c2>> :> c2
   @@ <<c2, c3>> :> s12 @@ <<c2, c4>> :> s12 @@ <<c3, r>> :> s34
   @@ <<c3, s12>> :> s34 @@ <<c3, s34>> :> s34 @@ <<c3, c1>> :> s34
   @@ <<c3, c2>> :> s34 @@ <<c3, c3>> :> c3 @@ <<c3, c4>> :> s34
   @@ <<c4, r>> :> s34 @@ <<c4, s12>> :> s34 @@ <<c4, s34>> :> s34
   @@ <<c4, c1>> :> s34 @@ <<c4, c2>> :> s34 @@ <<c4, c3>> :> s34
   @@ <<c4, c4>> :> c4
REQ_SET == 0 .. (REQ_NUM - 1)
AGG_SET == 0 .. (AGG_NUM - 1)

__IsReliableLink(__src, __dst) == <<__src, __dst>> \in __reliable_links
__IsUnreliableLink(__src, __dst) == <<__src, __dst>> \notin __reliable_links
__MinPsnElem(__S) == CHOOSE __x \in __S : \A __y \in __S : __x.psn <= __y.psn
__Set2OrderedSeq(__S) ==
  LET
    RECURSIVE F(_, _)
    F(__res, __SS) == IF __SS = {}
      THEN __res
      ELSE LET __x == __MinPsnElem(__SS) IN F(Append(__res, __x), __SS \ {__x})
  IN F(<<>>, __S)
__OutOfOrderRange(__seq) == 1..Len(__seq)
__InsertAtEnd(__seq, __i, __elem) == InsertAt(__seq, Len(__seq) - __i + 1, __elem)
__Receive(__n) == Head(__net_buf[__n])
__Node(__thread) == Head(__thread)
__PosCount(__f) == Cardinality({__x \in DOMAIN __f : __f[__x] > 0})
__AllPossibleLoss(__S) == {__s \in SUBSET __S : Cardinality(__s) <= __max_loss}
__AllPossibleOutOfOrder(__S) ==
  LET
    __max_range == IF __S = {} THEN {} ELSE 0..Max({Len(__net_buf[__i]) : __i \in __S})
    __ooo_set == [__S -> __max_range]
    __ooo_set_possible == {__i \in __ooo_set :
      /\ (\A __j \in DOMAIN __i : __i[__j] \in __OutOfOrderRange(__net_buf[__j]) \cup {0})
      /\ __PosCount(__i) <= __max_out_of_order
    }
  IN __ooo_set_possible
__AllPossibleSeq(__S) == {__seq \in [1..Cardinality(__S) -> __S] : IsInjective(__seq)}
__NodeTerminated(__n) == __active_threads[__n] <= 0
SmallerOf(a, b) == IF a < b THEN a ELSE b
LargerOf(a, b) == IF a > b THEN a ELSE b
ParentOf(nid) == __next_hop[nid, COMM_ROOT]
ChildrenOf(nid) == {nid \div 2, 2 * nid, 2 * nid + 1} \ {ParentOf(nid), 0}
MakePkt(type, src, dst, psn) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn]
MakePktWithValue(type, src, dst, psn, value) == [type |-> type, src |-> src, dst |-> dst, psn |-> psn, value |-> value]
Idx(psn) == psn % AGG_NUM
IsCompleted(nid, epsn, psn) == \A ch \in ChildrenOf(nid) : epsn[ch] > psn
MinEPsn(nid, epsn) == Min({epsn[ch] : ch \in ChildrenOf(nid)})
SwitchCanTerminate(nid) == \A c \in NODE_SET : c > nid => __NodeTerminated(c)
StrictRetxCondition(pkt) == \A nid \in NODE_SET : ~__NodeTerminated(nid) => __net_buf[nid] = <<>>
PsnSetToIdxSet(range) == {Idx(psn) : psn \in range}
RangeToSeq(start, stop) == [i \in 1 .. (stop - start + 1) |-> i + start - 1]


vars == << pc, __net_buf, __max_loss, __max_out_of_order, __max_duplication, 
           req_val, res, res_epsn, __active_threads, AggEPsn, AggBuffer, 
           AggPsnStart, nak_sent, recirc_pkts, requests, base >>

ProcSet == ((CLIENT_SET \X {"ClientInitSend"})) \cup ((CLIENT_SET \X {"ClientRecv"})) \cup ((CLIENT_SET \X {"ClientRetx"})) \cup ((SWITCH_SET \X {"SwitchRecv"})) \cup ((SWITCH_SET \X {"SwitchRecirc"})) \cup ((SWITCH_SET \X {"SwitchRetxReq"}))

Init == (* Global variables *)
        /\ __net_buf = [__n \in NODE_SET |-> <<>>]
        /\ __max_loss = MAX_LOSS
        /\ __max_out_of_order = MAX_OUT_OF_ORDER
        /\ __max_duplication = MAX_DUPLICATION
        /\ req_val = [c \in CLIENT_SET, i \in REQ_SET |-> RandomElement(0 .. 100000)]
        /\ res = [i \in REQ_SET |-> SumSet({req_val[c, i] : c \in CLIENT_SET \ {COMM_ROOT}})]
        /\ res_epsn = 0
        /\ __active_threads = [__n \in SWITCH_SET |-> 3] @@ [__n \in CLIENT_SET |-> 3]
        /\ AggEPsn = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> 0])]
        /\ AggBuffer = [__n \in SWITCH_SET |-> ([i \in AGG_SET |-> 0])]
        /\ AggPsnStart = [__n \in SWITCH_SET |-> (0)]
        /\ nak_sent = [__n \in SWITCH_SET |-> ([ch \in ChildrenOf(__n) |-> FALSE])]
        /\ recirc_pkts = [__n \in SWITCH_SET |-> (<<>>)]
        /\ requests = [__n \in CLIENT_SET |-> ([i \in REQ_SET |-> MakePktWithValue(REQ, __n, ParentOf(__n), i, req_val[__n, i])])]
        /\ base = [__n \in CLIENT_SET |-> (- 1)]
        /\ pc = [self \in ProcSet |-> CASE self \in (CLIENT_SET \X {"ClientInitSend"}) -> "L_ClientInitSend"
                                        [] self \in (CLIENT_SET \X {"ClientRecv"}) -> "L_ClientRecv"
                                        [] self \in (CLIENT_SET \X {"ClientRetx"}) -> "L_ClientRetx"
                                        [] self \in (SWITCH_SET \X {"SwitchRecv"}) -> "L_SwitchRecv"
                                        [] self \in (SWITCH_SET \X {"SwitchRecirc"}) -> "L_SwitchRecirc"
                                        [] self \in (SWITCH_SET \X {"SwitchRetxReq"}) -> "L_SwitchRetxReq"]

L_ClientInitSend(self) == /\ pc[self] = "L_ClientInitSend"
                          /\ IF __active_threads[__Node(self)] <= 0
                                THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                     /\ UNCHANGED << __net_buf, __max_loss, 
                                                     __max_out_of_order, base >>
                                ELSE /\ IF __Node(self) # COMM_ROOT
                                           THEN /\ LET end_idx == SmallerOf(WINDOW_SIZE, REQ_NUM) IN
                                                     /\ LET __keys == DOMAIN ([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]]) IN
                                                          LET __dst == ([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                            LET __h == __next_hop[__Node(self), __dst] IN
                                                              LET __pkts_ordered == __Set2OrderedSeq({([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys}) IN
                                                                /\ LET __b == Assert((Cardinality(__keys) > 0), "Unicast: empty packets") IN
                                                                     Assert(__b, 
                                                                            "Failure of assertion at line 121, column 7 of macro called at line 377, column 11.")
                                                                /\ LET __b == Assert((\A __i \in __keys : ([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i].dst = __dst), "Unicast: different destinations") IN
                                                                     Assert(__b, 
                                                                            "Failure of assertion at line 121, column 7 of macro called at line 377, column 11.")
                                                                /\ IF __IsReliableLink(__Node(self), __h)
                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered]
                                                                           /\ UNCHANGED << __max_loss, 
                                                                                           __max_out_of_order >>
                                                                      ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                                LET __unlost_pkts == {([i \in 0 .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys \ __loss} IN
                                                                                  LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                    /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                    /\ \/ /\ __max_out_of_order > 0
                                                                                          /\ Cardinality(__unlost_pkts) >= 2
                                                                                          /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                               /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                               /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo]
                                                                                       \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered]
                                                                                          /\ UNCHANGED __max_out_of_order
                                                     /\ base' = [base EXCEPT ![__Node(self)] = 0]
                                           ELSE /\ TRUE
                                                /\ UNCHANGED << __net_buf, 
                                                                __max_loss, 
                                                                __max_out_of_order, 
                                                                base >>
                                     /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << __max_duplication, req_val, res, 
                                          res_epsn, __active_threads, AggEPsn, 
                                          AggBuffer, AggPsnStart, nak_sent, 
                                          recirc_pkts, requests >>

ClientInitSend(self) == L_ClientInitSend(self)

L_ClientRecv(self) == /\ pc[self] = "L_ClientRecv"
                      /\ IF __active_threads[__Node(self)] <= 0
                            THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order, res_epsn, 
                                                 __active_threads, base >>
                            ELSE /\ LET pkt == __Receive(__Node(self)) IN
                                      /\ __net_buf[__Node(self)] # <<>>
                                      /\ IF pkt.type = REQ
                                            THEN /\ LET __b == Assert((__Node(self) = COMM_ROOT), "ClientReq: non-root client receives request.") IN
                                                      Assert(__b, 
                                                             "Failure of assertion at line 121, column 7 of macro called at line 398, column 13.")
                                                 /\ LET __b == Assert(((pkt).value = res[(pkt).psn]), "ClientReq: incorrect result.") IN
                                                      Assert(__b, 
                                                             "Failure of assertion at line 121, column 7 of macro called at line 399, column 13.")
                                                 /\ IF (pkt).psn = res_epsn
                                                       THEN /\ LET next == __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn))).dst] IN
                                                                 /\ res_epsn' = res_epsn + 1
                                                                 /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'))))
                                                                       THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 406, column 19.")
                                                                            /\ LET __s == __Node(self) IN
                                                                                 LET __h == __next_hop[__s, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn')))).dst] IN
                                                                                   \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                      /\ __max_out_of_order > 0
                                                                                      /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                      /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                           IF __s = __h
                                                                                              THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'))))))]
                                                                                              ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'))))),
                                                                                                                                     ![__s] = Tail(@)]
                                                                                      /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                      /\ UNCHANGED __max_loss
                                                                                   \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                      /\ __max_loss > 0
                                                                                      /\ __max_loss' = __max_loss - 1
                                                                                      /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                      /\ UNCHANGED __max_out_of_order
                                                                                   \/ /\ IF __s = __h
                                                                                            THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'))))))]
                                                                                            ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((ACK), (__Node(self)), ((pkt).src), (res_epsn'))))),
                                                                                                                                   ![__s] = Tail(@)]
                                                                                      /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                       ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 409, column 19.")
                                                                            /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                            /\ UNCHANGED << __max_loss, 
                                                                                            __max_out_of_order >>
                                                                 /\ IF res_epsn' = REQ_NUM
                                                                       THEN /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                                                            /\ pc' = [pc EXCEPT ![self] = "Done"]
                                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                            /\ UNCHANGED __active_threads
                                                       ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 121, column 7 of macro called at line 417, column 15.")
                                                            /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                            /\ UNCHANGED << __max_loss, 
                                                                            __max_out_of_order, 
                                                                            res_epsn, 
                                                                            __active_threads >>
                                                 /\ base' = base
                                            ELSE /\ IF pkt.type = ACK
                                                       THEN /\ LET __b == Assert((__Node(self) # COMM_ROOT), "ClientAck: root client receives ACK.") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 121, column 7 of macro called at line 421, column 13.")
                                                            /\ IF (pkt).psn > base[__Node(self)]
                                                                  THEN /\ LET start_idx == base[__Node(self)] + WINDOW_SIZE IN
                                                                            LET end_idx == SmallerOf((pkt).psn + WINDOW_SIZE, REQ_NUM) IN
                                                                              /\ IF start_idx < end_idx
                                                                                    THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropUnicast: empty buffer") IN
                                                                                              Assert(__b, 
                                                                                                     "Failure of assertion at line 121, column 7 of macro called at line 428, column 19.")
                                                                                         /\ LET __keys == DOMAIN ([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]]) IN
                                                                                              LET __dst == ([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                                                                LET __s == __Node(self) IN
                                                                                                  LET __h == __next_hop[__s, __dst] IN
                                                                                                    LET __pkts_ordered == __Set2OrderedSeq({([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys}) IN
                                                                                                      /\ LET __b == Assert((Cardinality(__keys) > 0), "DropUnicast: empty packets") IN
                                                                                                           Assert(__b, 
                                                                                                                  "Failure of assertion at line 121, column 7 of macro called at line 428, column 19.")
                                                                                                      /\ LET __b == Assert((\A __i \in __keys : ([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i].dst = __dst), "DropUnicast: different destinations") IN
                                                                                                           Assert(__b, 
                                                                                                                  "Failure of assertion at line 121, column 7 of macro called at line 428, column 19.")
                                                                                                      /\ IF __IsReliableLink(__s, __h)
                                                                                                            THEN /\ IF __s = __h
                                                                                                                       THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __pkts_ordered)]
                                                                                                                       ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered,
                                                                                                                                                              ![__s] = Tail(@)]
                                                                                                                 /\ UNCHANGED << __max_loss, 
                                                                                                                                 __max_out_of_order >>
                                                                                                            ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                                                                      LET __unlost_pkts == {([i \in start_idx .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys \ __loss} IN
                                                                                                                        LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                                                          /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                                                          /\ \/ /\ __max_out_of_order > 0
                                                                                                                                /\ Cardinality(__unlost_pkts) >= 2
                                                                                                                                /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                                                                     /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                                                     /\ IF __s = __h
                                                                                                                                           THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __ooo)]
                                                                                                                                           ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo,
                                                                                                                                                                                  ![__s] = Tail(@)]
                                                                                                                             \/ /\ IF __s = __h
                                                                                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(@ \o __ordered)]
                                                                                                                                      ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered,
                                                                                                                                                                             ![__s] = Tail(@)]
                                                                                                                                /\ UNCHANGED __max_out_of_order
                                                                                    ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                              Assert(__b, 
                                                                                                     "Failure of assertion at line 121, column 7 of macro called at line 431, column 19.")
                                                                                         /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                         /\ UNCHANGED << __max_loss, 
                                                                                                         __max_out_of_order >>
                                                                              /\ base' = [base EXCEPT ![__Node(self)] = (pkt).psn]
                                                                              /\ IF base'[__Node(self)] = REQ_NUM
                                                                                    THEN /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "Done"]
                                                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                                         /\ UNCHANGED __active_threads
                                                                  ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 121, column 7 of macro called at line 440, column 15.")
                                                                       /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order, 
                                                                                       __active_threads, 
                                                                                       base >>
                                                       ELSE /\ IF pkt.type = NAK
                                                                  THEN /\ LET __b == Assert((__Node(self) # COMM_ROOT), "ClientNak: root client receives NAK.") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 121, column 7 of macro called at line 444, column 13.")
                                                                       /\ IF (pkt).psn >= base[__Node(self)]
                                                                             THEN /\ LET next == __next_hop[__Node(self), (requests[__Node(self)][(pkt).psn]).dst] IN
                                                                                       IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (requests[__Node(self)][(pkt).psn]))
                                                                                          THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                    Assert(__b, 
                                                                                                           "Failure of assertion at line 121, column 7 of macro called at line 450, column 19.")
                                                                                               /\ LET __s == __Node(self) IN
                                                                                                    LET __h == __next_hop[__s, ((requests[__Node(self)][(pkt).psn])).dst] IN
                                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                         /\ __max_out_of_order > 0
                                                                                                         /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                         /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                              IF __s = __h
                                                                                                                 THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((requests[__Node(self)][(pkt).psn]))))]
                                                                                                                 ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((requests[__Node(self)][(pkt).psn]))),
                                                                                                                                                        ![__s] = Tail(@)]
                                                                                                         /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                         /\ UNCHANGED __max_loss
                                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                         /\ __max_loss > 0
                                                                                                         /\ __max_loss' = __max_loss - 1
                                                                                                         /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                         /\ UNCHANGED __max_out_of_order
                                                                                                      \/ /\ IF __s = __h
                                                                                                               THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((requests[__Node(self)][(pkt).psn]))))]
                                                                                                               ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((requests[__Node(self)][(pkt).psn]))),
                                                                                                                                                      ![__s] = Tail(@)]
                                                                                                         /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                          ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                    Assert(__b, 
                                                                                                           "Failure of assertion at line 121, column 7 of macro called at line 453, column 19.")
                                                                                               /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                               /\ UNCHANGED << __max_loss, 
                                                                                                               __max_out_of_order >>
                                                                             ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 121, column 7 of macro called at line 458, column 15.")
                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                  /\ UNCHANGED << __max_loss, 
                                                                                                  __max_out_of_order >>
                                                                  ELSE /\ LET __b == Assert(FALSE, "ClientRecv: unexpected packet type") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 121, column 7 of macro called at line 462, column 13.")
                                                                       /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                            Assert(__b, 
                                                                                   "Failure of assertion at line 121, column 7 of macro called at line 463, column 13.")
                                                                       /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order >>
                                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRecv"]
                                                            /\ UNCHANGED << __active_threads, 
                                                                            base >>
                                                 /\ UNCHANGED res_epsn
                      /\ UNCHANGED << __max_duplication, req_val, res, AggEPsn, 
                                      AggBuffer, AggPsnStart, nak_sent, 
                                      recirc_pkts, requests >>

ClientRecv(self) == L_ClientRecv(self)

L_ClientRetx(self) == /\ pc[self] = "L_ClientRetx"
                      /\ IF __Node(self) # COMM_ROOT /\ base[__Node(self)] < REQ_NUM
                            THEN /\ IF __active_threads[__Node(self)] <= 0
                                       THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                            /\ UNCHANGED << __net_buf, 
                                                            __max_loss, 
                                                            __max_out_of_order >>
                                       ELSE /\ LET end_idx == SmallerOf(base[__Node(self)] + WINDOW_SIZE, REQ_NUM) IN
                                                 /\ __net_buf[__Node(self)] = <<>>
                                                 /\ base[__Node(self)] >= 0
                                                 /\ StrictRetxCondition(0)
                                                 /\ LET __keys == DOMAIN ([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]]) IN
                                                      LET __dst == ([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]])[CHOOSE __i \in __keys : TRUE].dst IN
                                                        LET __h == __next_hop[__Node(self), __dst] IN
                                                          LET __pkts_ordered == __Set2OrderedSeq({([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys}) IN
                                                            /\ LET __b == Assert((Cardinality(__keys) > 0), "Unicast: empty packets") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 121, column 7 of macro called at line 485, column 11.")
                                                            /\ LET __b == Assert((\A __i \in __keys : ([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i].dst = __dst), "Unicast: different destinations") IN
                                                                 Assert(__b, 
                                                                        "Failure of assertion at line 121, column 7 of macro called at line 485, column 11.")
                                                            /\ IF __IsReliableLink(__Node(self), __h)
                                                                  THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __pkts_ordered]
                                                                       /\ UNCHANGED << __max_loss, 
                                                                                       __max_out_of_order >>
                                                                  ELSE /\ \E __loss \in __AllPossibleLoss(__keys):
                                                                            LET __unlost_pkts == {([i \in base[__Node(self)] .. (end_idx - 1) |-> requests[__Node(self)][i]])[__i] : __i \in __keys \ __loss} IN
                                                                              LET __ordered == __Set2OrderedSeq(__unlost_pkts) IN
                                                                                /\ __max_loss' = __max_loss - Cardinality(__loss)
                                                                                /\ \/ /\ __max_out_of_order > 0
                                                                                      /\ Cardinality(__unlost_pkts) >= 2
                                                                                      /\ \E __ooo \in __AllPossibleSeq(__unlost_pkts) \ {__ordered}:
                                                                                           /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                           /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ooo]
                                                                                   \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = @ \o __ordered]
                                                                                      /\ UNCHANGED __max_out_of_order
                                            /\ pc' = [pc EXCEPT ![self] = "L_ClientRetx"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order >>
                      /\ UNCHANGED << __max_duplication, req_val, res, 
                                      res_epsn, __active_threads, AggEPsn, 
                                      AggBuffer, AggPsnStart, nak_sent, 
                                      recirc_pkts, requests, base >>

ClientRetx(self) == L_ClientRetx(self)

L_SwitchRecv(self) == /\ pc[self] = "L_SwitchRecv"
                      /\ IF ~SwitchCanTerminate(__Node(self))
                            THEN /\ IF __active_threads[__Node(self)] <= 0
                                       THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                            /\ UNCHANGED << __net_buf, 
                                                            __max_loss, 
                                                            __max_out_of_order, 
                                                            AggEPsn, AggBuffer, 
                                                            AggPsnStart, 
                                                            nak_sent, 
                                                            recirc_pkts >>
                                       ELSE /\ LET pkt == __Receive(__Node(self)) IN
                                                 /\ recirc_pkts[__Node(self)] = <<>>
                                                 /\ __net_buf[__Node(self)] # <<>>
                                                 /\ IF pkt.type = REQ
                                                       THEN /\ LET idx == Idx((pkt).psn) IN
                                                                 LET ch == (pkt).src IN
                                                                   /\ LET __b == Assert(((pkt).src \in ChildrenOf(__Node(self))), "SwitchReq: switch receives request from non-child.") IN
                                                                        Assert(__b, 
                                                                               "Failure of assertion at line 121, column 7 of macro called at line 510, column 15.")
                                                                   /\ IF (pkt).psn < AggEPsn[__Node(self)][ch]
                                                                         THEN /\ LET next == __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst] IN
                                                                                   IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))
                                                                                      THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                Assert(__b, 
                                                                                                       "Failure of assertion at line 121, column 7 of macro called at line 516, column 21.")
                                                                                           /\ LET __s == __Node(self) IN
                                                                                                LET __h == __next_hop[__s, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch])))).dst] IN
                                                                                                  \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                     /\ __max_out_of_order > 0
                                                                                                     /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                     /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                          IF __s = __h
                                                                                                             THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))))]
                                                                                                             ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))),
                                                                                                                                                    ![__s] = Tail(@)]
                                                                                                     /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                     /\ UNCHANGED __max_loss
                                                                                                  \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                     /\ __max_loss > 0
                                                                                                     /\ __max_loss' = __max_loss - 1
                                                                                                     /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                     /\ UNCHANGED __max_out_of_order
                                                                                                  \/ /\ IF __s = __h
                                                                                                           THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))))]
                                                                                                           ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))),
                                                                                                                                                  ![__s] = Tail(@)]
                                                                                                     /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                      ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                Assert(__b, 
                                                                                                       "Failure of assertion at line 121, column 7 of macro called at line 519, column 21.")
                                                                                           /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                           /\ UNCHANGED << __max_loss, 
                                                                                                           __max_out_of_order >>
                                                                              /\ UNCHANGED << AggEPsn, 
                                                                                              AggBuffer, 
                                                                                              nak_sent, 
                                                                                              recirc_pkts >>
                                                                         ELSE /\ IF (pkt).psn > AggEPsn[__Node(self)][ch]
                                                                                    THEN /\ IF ~nak_sent[__Node(self)][ch]
                                                                                               THEN /\ LET next == __next_hop[__Node(self), (MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst] IN
                                                                                                         /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))
                                                                                                               THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                                         Assert(__b, 
                                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 529, column 23.")
                                                                                                                    /\ LET __s == __Node(self) IN
                                                                                                                         LET __h == __next_hop[__s, ((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch])))).dst] IN
                                                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                              /\ __max_out_of_order > 0
                                                                                                                              /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                                              /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                                                   IF __s = __h
                                                                                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))))]
                                                                                                                                      ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))),
                                                                                                                                                                             ![__s] = Tail(@)]
                                                                                                                              /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                                              /\ UNCHANGED __max_loss
                                                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                              /\ __max_loss > 0
                                                                                                                              /\ __max_loss' = __max_loss - 1
                                                                                                                              /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                                              /\ UNCHANGED __max_out_of_order
                                                                                                                           \/ /\ IF __s = __h
                                                                                                                                    THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))))]
                                                                                                                                    ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((NAK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))))),
                                                                                                                                                                           ![__s] = Tail(@)]
                                                                                                                              /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                                               ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                                         Assert(__b, 
                                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 532, column 23.")
                                                                                                                    /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                                    /\ UNCHANGED << __max_loss, 
                                                                                                                                    __max_out_of_order >>
                                                                                                         /\ nak_sent' = [nak_sent EXCEPT ![__Node(self)][ch] = TRUE]
                                                                                               ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                         Assert(__b, 
                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 538, column 19.")
                                                                                                    /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                    /\ UNCHANGED << __max_loss, 
                                                                                                                    __max_out_of_order, 
                                                                                                                    nak_sent >>
                                                                                         /\ UNCHANGED << AggEPsn, 
                                                                                                         AggBuffer, 
                                                                                                         recirc_pkts >>
                                                                                    ELSE /\ nak_sent' = [nak_sent EXCEPT ![__Node(self)][ch] = FALSE]
                                                                                         /\ IF (pkt).psn >= AggPsnStart[__Node(self)] + AGG_NUM
                                                                                               THEN /\ TRUE
                                                                                                    /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                         Assert(__b, 
                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 545, column 19.")
                                                                                                    /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                    /\ UNCHANGED << __max_loss, 
                                                                                                                    __max_out_of_order, 
                                                                                                                    AggEPsn, 
                                                                                                                    AggBuffer, 
                                                                                                                    recirc_pkts >>
                                                                                               ELSE /\ LET next == __next_hop[__Node(self), (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn[__Node(self)][ch]))).dst] IN
                                                                                                         /\ AggBuffer' = [AggBuffer EXCEPT ![__Node(self)][idx] = AggBuffer[__Node(self)][idx] + (pkt).value]
                                                                                                         /\ AggEPsn' = [AggEPsn EXCEPT ![__Node(self)][ch] = AggEPsn[__Node(self)][ch] + 1]
                                                                                                         /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch]))))
                                                                                                               THEN /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "DropSend: empty buffer") IN
                                                                                                                         Assert(__b, 
                                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 554, column 23.")
                                                                                                                    /\ LET __s == __Node(self) IN
                                                                                                                         LET __h == __next_hop[__s, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch])))).dst] IN
                                                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                              /\ __max_out_of_order > 0
                                                                                                                              /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                                                              /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                                                                   IF __s = __h
                                                                                                                                      THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(__InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch]))))))]
                                                                                                                                      ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch]))))),
                                                                                                                                                                             ![__s] = Tail(@)]
                                                                                                                              /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                                                              /\ UNCHANGED __max_loss
                                                                                                                           \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                                                              /\ __max_loss > 0
                                                                                                                              /\ __max_loss' = __max_loss - 1
                                                                                                                              /\ __net_buf' = [__net_buf EXCEPT ![__s] = Tail(@)]
                                                                                                                              /\ UNCHANGED __max_out_of_order
                                                                                                                           \/ /\ IF __s = __h
                                                                                                                                    THEN /\ __net_buf' = [__net_buf EXCEPT ![__h] = Tail(Append(@, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch]))))))]
                                                                                                                                    ELSE /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((MakePkt((ACK), (__Node(self)), (ch), (AggEPsn'[__Node(self)][ch]))))),
                                                                                                                                                                           ![__s] = Tail(@)]
                                                                                                                              /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                                                               ELSE /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                                         Assert(__b, 
                                                                                                                                "Failure of assertion at line 121, column 7 of macro called at line 557, column 23.")
                                                                                                                    /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                                                    /\ UNCHANGED << __max_loss, 
                                                                                                                                    __max_out_of_order >>
                                                                                                         /\ IF IsCompleted(__Node(self), AggEPsn'[__Node(self)], (pkt).psn)
                                                                                                               THEN /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = Append(recirc_pkts[__Node(self)], (MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), (pkt).psn, AggBuffer'[__Node(self)][idx])))]
                                                                                                               ELSE /\ TRUE
                                                                                                                    /\ UNCHANGED recirc_pkts
                                                            /\ UNCHANGED AggPsnStart
                                                       ELSE /\ IF pkt.type = ACK
                                                                  THEN /\ LET idx == Idx((pkt).psn) IN
                                                                            /\ LET __b == Assert(((pkt).src = ParentOf(__Node(self))), "SwitchAck: switch receives ACK from non-parent.") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 571, column 15.")
                                                                            /\ LET __b == Assert((\A ch \in ChildrenOf(__Node(self)) : AggEPsn[__Node(self)][ch] >= (pkt).psn), "SwitchAck: switch receives ACK for uncompleted aggregation.") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 572, column 15.")
                                                                            /\ LET __b == Assert(((pkt).psn <= AggPsnStart[__Node(self)] + AGG_NUM), "SwitchAck: ACK for unstarted aggregation.") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 573, column 15.")
                                                                            /\ IF (pkt).psn > AggPsnStart[__Node(self)]
                                                                                  THEN /\ LET range == AggPsnStart[__Node(self)] .. ((pkt).psn - 1) IN
                                                                                            /\ AggBuffer' = [AggBuffer EXCEPT ![__Node(self)] = [i \in PsnSetToIdxSet(range) |-> 0] @@ AggBuffer[__Node(self)]]
                                                                                            /\ AggPsnStart' = [AggPsnStart EXCEPT ![__Node(self)] = (pkt).psn]
                                                                                  ELSE /\ TRUE
                                                                                       /\ UNCHANGED << AggBuffer, 
                                                                                                       AggPsnStart >>
                                                                            /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                 Assert(__b, 
                                                                                        "Failure of assertion at line 121, column 7 of macro called at line 582, column 15.")
                                                                            /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                       /\ UNCHANGED recirc_pkts
                                                                  ELSE /\ IF pkt.type = NAK
                                                                             THEN /\ LET idx == Idx((pkt).psn) IN
                                                                                       LET max_psn == MinEPsn(__Node(self), AggEPsn[__Node(self)]) - 1 IN
                                                                                         LET range_seq == RangeToSeq(AggPsnStart[__Node(self)], max_psn) IN
                                                                                           LET pkts == [i \in 1 .. Len(range_seq) |-> MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), range_seq[i], AggBuffer[__Node(self)][Idx(range_seq[i])])] IN
                                                                                             /\ LET __b == Assert(((pkt).src = ParentOf(__Node(self))), "SwitchNak: switch receives NAK from non-parent.") IN
                                                                                                  Assert(__b, 
                                                                                                         "Failure of assertion at line 121, column 7 of macro called at line 592, column 15.")
                                                                                             /\ LET __b == Assert(((pkt).psn = AggPsnStart[__Node(self)]), "SwitchNak: we currently do not consider out-of-order networks.") IN
                                                                                                  Assert(__b, 
                                                                                                         "Failure of assertion at line 121, column 7 of macro called at line 593, column 15.")
                                                                                             /\ LET __b == Assert((IsCompleted(__Node(self), AggEPsn[__Node(self)], (pkt).psn)), "SwitchNak: NAK for uncompleted aggregation.") IN
                                                                                                  Assert(__b, 
                                                                                                         "Failure of assertion at line 121, column 7 of macro called at line 594, column 15.")
                                                                                             /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = recirc_pkts[__Node(self)] \o (pkts)]
                                                                                             /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                                  Assert(__b, 
                                                                                                         "Failure of assertion at line 121, column 7 of macro called at line 596, column 15.")
                                                                                             /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                             ELSE /\ LET __b == Assert(FALSE, "SwitchRecv: unexpected packet type") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 121, column 7 of macro called at line 600, column 13.")
                                                                                  /\ LET __b == Assert((__net_buf[__Node(self)] # <<>>), "Drop: empty buffer") IN
                                                                                       Assert(__b, 
                                                                                              "Failure of assertion at line 121, column 7 of macro called at line 601, column 13.")
                                                                                  /\ __net_buf' = [__net_buf EXCEPT ![__Node(self)] = Tail(@)]
                                                                                  /\ UNCHANGED recirc_pkts
                                                                       /\ UNCHANGED << AggBuffer, 
                                                                                       AggPsnStart >>
                                                            /\ UNCHANGED << __max_loss, 
                                                                            __max_out_of_order, 
                                                                            AggEPsn, 
                                                                            nak_sent >>
                                            /\ pc' = [pc EXCEPT ![self] = "L_SwitchRecv"]
                                 /\ UNCHANGED __active_threads
                            ELSE /\ __active_threads' = [__active_threads EXCEPT ![__Node(self)] = 0]
                                 /\ pc' = [pc EXCEPT ![self] = "Done"]
                                 /\ UNCHANGED << __net_buf, __max_loss, 
                                                 __max_out_of_order, AggEPsn, 
                                                 AggBuffer, AggPsnStart, 
                                                 nak_sent, recirc_pkts >>
                      /\ UNCHANGED << __max_duplication, req_val, res, 
                                      res_epsn, requests, base >>

SwitchRecv(self) == L_SwitchRecv(self)

L_SwitchRecirc(self) == /\ pc[self] = "L_SwitchRecirc"
                        /\ IF ~SwitchCanTerminate(__Node(self))
                              THEN /\ IF __active_threads[__Node(self)] <= 0
                                         THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                              /\ UNCHANGED << __net_buf, 
                                                              __max_loss, 
                                                              __max_out_of_order, 
                                                              __max_duplication, 
                                                              recirc_pkts >>
                                         ELSE /\ LET next == __next_hop[__Node(self), (Head(recirc_pkts[__Node(self)])).dst] IN
                                                   /\ recirc_pkts[__Node(self)] # <<>>
                                                   /\ IF ~__NodeTerminated(next) /\ ~Contains(__net_buf[next], (Head(recirc_pkts[__Node(self)])))
                                                         THEN /\ LET has_dup == /\ "id" \in DOMAIN ((Head(recirc_pkts[__Node(self)])))
                                                                                /\ \E __n \in NODE_SET : \E __i \in DOMAIN __net_buf[__n] :
                                                                                  "id" \in DOMAIN __net_buf[__n][__i] /\ __net_buf[__n][__i].id = ((Head(recirc_pkts[__Node(self)]))).id IN
                                                                   IF ~has_dup
                                                                      THEN /\ LET __h == __next_hop[__Node(self), ((Head(recirc_pkts[__Node(self)]))).dst] IN
                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                   /\ __max_out_of_order > 0
                                                                                   /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                   /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                        __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                   /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                   /\ UNCHANGED __max_loss
                                                                                \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                   /\ __max_loss > 0
                                                                                   /\ __max_loss' = __max_loss - 1
                                                                                   /\ UNCHANGED <<__net_buf, __max_out_of_order>>
                                                                                \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                   /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                           /\ UNCHANGED __max_duplication
                                                                      ELSE /\ \/ /\ __max_duplication > 0
                                                                                 /\ __max_duplication' = __max_duplication - 1
                                                                                 /\ LET __h == __next_hop[__Node(self), ((Head(recirc_pkts[__Node(self)]))).dst] IN
                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                         /\ __max_out_of_order > 0
                                                                                         /\ __OutOfOrderRange(__net_buf[__h]) # {}
                                                                                         /\ \E __pos \in __OutOfOrderRange(__net_buf[__h]):
                                                                                              __net_buf' = [__net_buf EXCEPT ![__h] = __InsertAtEnd(@, __pos, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                         /\ __max_out_of_order' = __max_out_of_order - 1
                                                                                         /\ UNCHANGED __max_loss
                                                                                      \/ /\ __IsUnreliableLink(__Node(self), __h)
                                                                                         /\ __max_loss > 0
                                                                                         /\ __max_loss' = __max_loss - 1
                                                                                         /\ UNCHANGED <<__net_buf, __max_out_of_order>>
                                                                                      \/ /\ __net_buf' = [__net_buf EXCEPT ![__h] = Append(@, ((Head(recirc_pkts[__Node(self)]))))]
                                                                                         /\ UNCHANGED <<__max_loss, __max_out_of_order>>
                                                                              \/ /\ TRUE
                                                                                 /\ UNCHANGED <<__net_buf, __max_loss, __max_out_of_order, __max_duplication>>
                                                         ELSE /\ TRUE
                                                              /\ UNCHANGED << __net_buf, 
                                                                              __max_loss, 
                                                                              __max_out_of_order, 
                                                                              __max_duplication >>
                                                   /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = Tail(recirc_pkts[__Node(self)])]
                                              /\ pc' = [pc EXCEPT ![self] = "L_SwitchRecirc"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                   /\ UNCHANGED << __net_buf, __max_loss, 
                                                   __max_out_of_order, 
                                                   __max_duplication, 
                                                   recirc_pkts >>
                        /\ UNCHANGED << req_val, res, res_epsn, 
                                        __active_threads, AggEPsn, AggBuffer, 
                                        AggPsnStart, nak_sent, requests, base >>

SwitchRecirc(self) == L_SwitchRecirc(self)

L_SwitchRetxReq(self) == /\ pc[self] = "L_SwitchRetxReq"
                         /\ IF ~SwitchCanTerminate(__Node(self))
                               THEN /\ IF __active_threads[__Node(self)] <= 0
                                          THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                                               /\ UNCHANGED recirc_pkts
                                          ELSE /\ LET max_psn == MinEPsn(__Node(self), AggEPsn[__Node(self)]) - 1 IN
                                                    LET range_seq == RangeToSeq(AggPsnStart[__Node(self)], max_psn) IN
                                                      LET pkts == [i \in 1 .. Len(range_seq) |-> MakePktWithValue(REQ, __Node(self), ParentOf(__Node(self)), range_seq[i], AggBuffer[__Node(self)][Idx(range_seq[i])])] IN
                                                        /\ recirc_pkts[__Node(self)] = <<>>
                                                        /\ StrictRetxCondition(0)
                                                        /\ IsCompleted(__Node(self), AggEPsn[__Node(self)], AggPsnStart[__Node(self)])
                                                        /\ recirc_pkts' = [recirc_pkts EXCEPT ![__Node(self)] = recirc_pkts[__Node(self)] \o (pkts)]
                                               /\ pc' = [pc EXCEPT ![self] = "L_SwitchRetxReq"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ UNCHANGED recirc_pkts
                         /\ UNCHANGED << __net_buf, __max_loss, 
                                         __max_out_of_order, __max_duplication, 
                                         req_val, res, res_epsn, 
                                         __active_threads, AggEPsn, AggBuffer, 
                                         AggPsnStart, nak_sent, requests, base >>

SwitchRetxReq(self) == L_SwitchRetxReq(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in (CLIENT_SET \X {"ClientInitSend"}): ClientInitSend(self))
           \/ (\E self \in (CLIENT_SET \X {"ClientRecv"}): ClientRecv(self))
           \/ (\E self \in (CLIENT_SET \X {"ClientRetx"}): ClientRetx(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRecv"}): SwitchRecv(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRecirc"}): SwitchRecirc(self))
           \/ (\E self \in (SWITCH_SET \X {"SwitchRetxReq"}): SwitchRetxReq(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Next)
        /\ \A self \in (CLIENT_SET \X {"ClientInitSend"}) : SF_vars(ClientInitSend(self))
        /\ \A self \in (CLIENT_SET \X {"ClientRecv"}) : SF_vars(ClientRecv(self))
        /\ \A self \in (CLIENT_SET \X {"ClientRetx"}) : SF_vars(ClientRetx(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRecv"}) : SF_vars(SwitchRecv(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRecirc"}) : SF_vars(SwitchRecirc(self))
        /\ \A self \in (SWITCH_SET \X {"SwitchRetxReq"}) : SF_vars(SwitchRetxReq(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

AggEPsnInRange == \A nid \in SWITCH_SET : \A ch \in ChildrenOf(nid) : AggEPsn[nid][ch] >= AggPsnStart[nid] /\ AggEPsn[nid][ch] <= AggPsnStart[nid] + AGG_NUM

====
