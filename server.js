const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const server = http.createServer((req,res)=>{
  if (req.url === "/health") {
    res.writeHead(200, {"Content-Type":"application/json","Cache-Control":"no-store"});
    return res.end(JSON.stringify({ok:true,service:"AkkCards",websocket:true,players:wss.clients.size}));
  }
  if (req.url === "/" || req.url.startsWith("/index.html")) {
    try {
      const html = fs.readFileSync(path.join(__dirname,"index.html"),"utf8");
      res.writeHead(200, {"Content-Type":"text/html; charset=utf-8","Cache-Control":"no-store"});
      return res.end(html);
    } catch (err) {
      res.writeHead(500, {"Content-Type":"text/plain"});
      return res.end("AkkCards frontend unavailable");
    }
  }
  res.writeHead(404, {"Content-Type":"application/json"});
  res.end(JSON.stringify({ok:false,error:"Not found"}));
});
const wss = new WebSocket.Server({server});
const players = new Map();
const tradeLine = new Map();
const waiting = [];
const sessions = new Map();
const matches = new Map();
const cards = new Map();

function send(ws,type,data={}) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({type,...data}));
}
function broadcast(type,data={},except=null) {
  for (const ws of wss.clients) if (ws !== except) send(ws,type,data);
}
function safeName(name) {
  return String(name||"Player").replace(/[^a-zA-Z0-9 _-]/g,"").slice(0,18) || "Player";
}
function removeFromQueue(ws) {
  const i=waiting.indexOf(ws);
  if(i>=0) waiting.splice(i,1);
}

wss.on("connection",(ws)=>{
  const id=Math.random().toString(36).slice(2,10);
  players.set(ws,{id,name:"Player-"+id});
  send(ws,"hello",{id,players:wss.clients.size});
  broadcast("presence",{players:wss.clients.size},ws);

  ws.on("message",(raw)=>{
    let msg;
    try { msg=JSON.parse(raw.toString()); } catch { return send(ws,"error",{message:"Invalid message"}); }

    if(msg.type==="identify"){
      sessions.set(players.get(ws).id,{money:20000,inventory:[],wins:0});

      players.get(ws).name=safeName(msg.name);
      return send(ws,"identified",{name:players.get(ws).name});
    }

    if(msg.type==="profile:state"){
      const p=players.get(ws), state=sessions.get(p.id);
      return send(ws,"profile:state",{state});
    }

    if(msg.type==="profile:save"){
      const p=players.get(ws);
      if(!sessions.has(p.id)) sessions.set(p.id,{money:20000,inventory:[],wins:0});
      const incoming=msg.state||{};
      const state=sessions.get(p.id);
      state.money=Math.max(0,Number(incoming.money)||0);
      state.inventory=Array.isArray(incoming.inventory)?incoming.inventory.slice(0,200):state.inventory;
      state.wins=Math.max(0,Number(incoming.wins)||0);
      return send(ws,"profile:saved",{state});
    }

    if(msg.type==="trade:create"){
      const p=players.get(ws);
      const ownerState=sessions.get(p.id);
      const listing={
        id:Math.random().toString(36).slice(2,10),
        ownerId:p.id, owner:p.name,
        card:msg.card, want:String(msg.want||"Open to offers").slice(0,160)
      };
      tradeLine.set(listing.id,listing);
      broadcast("trade:created",{listing});
      return;
    }

    if(msg.type==="trade:offer"){
      const listing=tradeLine.get(msg.listingId);
      if(!listing) return send(ws,"error",{message:"That listing no longer exists."});
      const from=players.get(ws);
      if(listing.ownerId===from.id) return send(ws,"error",{message:"You cannot offer on your own listing."});
      send(ws,"trade:offerSent",{listingId:listing.id});
      for(const client of wss.clients){
        const p=players.get(client);
        if(p && p.id===listing.ownerId) send(client,"trade:offer",{listing,from:from.name,offer:msg.offer});
      }
      return;
    }

    if(msg.type==="trade:respond"){
      const listing=tradeLine.get(msg.listingId);
      if(!listing) return send(ws,"error",{message:"Listing not found."});
      const p=players.get(ws);
      if(p.id!==listing.ownerId) return send(ws,"error",{message:"Only the listing owner can respond."});
      const target=[...wss.clients].find(c=>players.get(c)?.name===msg.from);
      if(target) send(target,"trade:response",{accepted:!!msg.accepted,listingId:listing.id});
      if(msg.accepted) {
        tradeLine.delete(listing.id);
        broadcast("trade:removed",{listingId:listing.id});
      }
      return;
    }

    if(msg.type==="battle:queue"){
      removeFromQueue(ws);
      waiting.push(ws);
      send(ws,"battle:queued",{playersWaiting:waiting.length});
      if(waiting.length>=2){
        const a=waiting.shift(), b=waiting.shift();
        if(!a || !b) return;
        const matchId=Math.random().toString(36).slice(2,10);
        const match={id:matchId,players:[a,b],cards:new Map()};
        matches.set(matchId,match);
        send(a,"battle:matched",{matchId,opponent:players.get(b)?.name||"Player"});
        send(b,"battle:matched",{matchId,opponent:players.get(a)?.name||"Player"});
      }
      return;
    }

    if(msg.type==="battle:card"){
      const matchId=String(msg.matchId||"");
      const match=matches.get(matchId);
      if(!match) return send(ws,"error",{message:"Match not found."});
      if(!match.players.includes(ws)) return send(ws,"error",{message:"You are not in this match."});
      match.cards.set(ws,msg.card);
      const other=match.players.find(c=>c!==ws);
      if(other && match.cards.has(other)){
        const a=match.cards.get(match.players[0]), b=match.cards.get(match.players[1]);
        const rank={Common:1,Uncommon:2,Rare:3,Epic:4,Legendary:5,Mythic:6,Secret:7,OG:8};
        const score=c=>((rank[c?.rarity]||1)+Math.random());
        const winner=score(a)>=score(b)?match.players[0]:match.players[1];
        const loser=winner===match.players[0]?match.players[1]:match.players[0];
        const winnerState=sessions.get(players.get(winner).id);
        winnerState.wins++;
        winnerState.money+=2500;
        send(match.players[0],"battle:result",{matchId,winner:players.get(winner).name,win:winner===match.players[0],reward:winner===match.players[0]?2500:0});
        send(match.players[1],"battle:result",{matchId,winner:players.get(winner).name,win:winner===match.players[1],reward:winner===match.players[1]?2500:0});
        matches.delete(matchId);
      }
      return;
    }

    if(msg.type==="ping") return send(ws,"pong",{time:Date.now()});
  });

  ws.on("close",()=>{
    removeFromQueue(ws);
    const p=players.get(ws);
    sessions.delete(p?.id);
    players.delete(ws);
    broadcast("presence",{players:wss.clients.size});
  });
});

server.listen(PORT,()=>console.log("AkkCards WebSocket listening on "+PORT));