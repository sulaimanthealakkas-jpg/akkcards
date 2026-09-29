const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const server = http.createServer((req,res)=>{
  res.writeHead(200, {"Content-Type":"application/json"});
  res.end(JSON.stringify({ok:true,service:"AkkCards WebSocket",players:wss.clients.size}));
});
const wss = new WebSocket.Server({server});
const players = new Map();
const tradeLine = new Map();
const waiting = [];

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
      players.get(ws).name=safeName(msg.name);
      return send(ws,"identified",{name:players.get(ws).name});
    }

    if(msg.type==="trade:create"){
      const p=players.get(ws);
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
        send(a,"battle:matched",{matchId,opponent:players.get(b)?.name||"Player"});
        send(b,"battle:matched",{matchId,opponent:players.get(a)?.name||"Player"});
      }
      return;
    }

    if(msg.type==="battle:card"){
      const matchId=String(msg.matchId||"");
      ws._battleCard={matchId,card:msg.card};
      const other=[...wss.clients].find(c=>c!==ws && c._battleCard?.matchId===matchId);
      if(other){
        send(ws,"battle:ready",{matchId,opponentCard:other._battleCard.card});
        send(other,"battle:ready",{matchId,opponentCard:ws._battleCard.card});
      }
      return;
    }

    if(msg.type==="ping") return send(ws,"pong",{time:Date.now()});
  });

  ws.on("close",()=>{
    removeFromQueue(ws);
    const p=players.get(ws);
    players.delete(ws);
    broadcast("presence",{players:wss.clients.size});
  });
});

server.listen(PORT,()=>console.log("AkkCards WebSocket listening on "+PORT));