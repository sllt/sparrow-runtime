// Real production-process oracle for immutable static table bindings.
// Standard library only; no cargo invocation, installation, or long soak.
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "net"
    "net/http"
    "net/http/httptest"
    "net/url"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "syscall"
    "time"
)

const token = "core-b-isolated-process-fixture"

func must(err error) { if err != nil { panic(err) } }
func require(ok bool, msg string) { if !ok { panic(msg) } }
func encoded(value any) []byte { b, e := json.Marshal(value); must(e); return b }
func save(path string, value any) { must(os.WriteFile(path, encoded(value), 0600)) }
func hash(path string) string { b,e:=os.ReadFile(path); must(e); sum:=sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func wait(label string, f func() bool) {
    end := time.Now().Add(15*time.Second)
    for time.Now().Before(end) { if f() { return }; time.Sleep(10*time.Millisecond) }
    panic("deadline: "+label)
}
func freePort() int { l,e:=net.Listen("tcp","127.0.0.1:0"); must(e); p:=l.Addr().(*net.TCPAddr).Port; must(l.Close()); return p }

type api struct { base string; client *http.Client }
func (a api) call(method, path string, body any, auth bool) (map[string]any,int) {
    var reader io.Reader
    if body != nil { reader=bytes.NewReader(encoded(body)) }
    req,e:=http.NewRequest(method,a.base+path,reader); must(e)
    if auth { req.Header.Set("Authorization","Bearer "+token) }
    req.Header.Set("Content-Type","application/json")
    if method==http.MethodPut && strings.HasPrefix(path,"/v1/pipelines/") {
        current, code:=a.call(http.MethodGet,path,nil,true)
        if code==200 { req.Header.Set("If-Match",fmt.Sprint(current["etag"])) }
    }
    response,e:=a.client.Do(req); if e!=nil { return nil,0 }; defer response.Body.Close()
    raw,e:=io.ReadAll(io.LimitReader(response.Body,2*1024*1024)); must(e)
    var out map[string]any
    if len(raw)>0 { must(json.Unmarshal(raw,&out)) }
    return out,response.StatusCode
}
func (a api) ok(method,path string, body any) map[string]any {
    out,code:=a.call(method,path,body,true)
    require(code>=200 && code<300,fmt.Sprintf("%s %s returned %d: %v",method,path,code,out)); return out
}
func (a api) rejected(method,path string, body any) map[string]any {
    out,code:=a.call(method,path,body,true)
    require(code>=400 && code<500,fmt.Sprintf("expected client rejection: %s %s code=%d %v",method,path,code,out)); return out
}

type child struct { cmd *exec.Cmd; log *os.File; done chan error }
func launch(binary, root, catalog, logfile string, port int) *child {
    log,e:=os.OpenFile(logfile,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0600); must(e)
    cmd:=exec.Command(binary,"--bind",fmt.Sprintf("127.0.0.1:%d",port),"--catalog",catalog,"--max-jobs","1","--safe-mode")
    cmd.Env=append(os.Environ(),"SPARROW_TOKEN="+token,"SPARROW_DATA_ROOTS="+root,
        "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef","SPARROW_REQUIRE_SECRETS_KEY=1")
    cmd.Stdout=log; cmd.Stderr=log; must(cmd.Start())
    c:=&child{cmd:cmd,log:log,done:make(chan error,1)}; go func(){c.done<-cmd.Wait()}(); return c
}
func (c *child) stop(signal os.Signal) {
    if c==nil || c.cmd==nil { return }
    _=c.cmd.Process.Signal(signal)
    select { case <-c.done: case <-time.After(8*time.Second): _=c.cmd.Process.Kill(); <-c.done }
    must(c.log.Close()); c.cmd=nil
}

type capture struct { sync.Mutex; rows []map[string]any }
func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close(); raw,e:=io.ReadAll(io.LimitReader(r.Body,1024*1024+1))
    if e!=nil || len(raw)>1024*1024 { w.WriteHeader(413); return }
    var rows []map[string]any
    if json.Unmarshal(raw,&rows)!=nil { w.WriteHeader(400); return }
    c.Lock(); if len(c.rows)+len(rows)>1024 { c.Unlock(); w.WriteHeader(413); return }; c.rows=append(c.rows,rows...); c.Unlock()
    w.WriteHeader(200)
}
func (c *capture) snapshot() []map[string]any { c.Lock(); defer c.Unlock(); return append([]map[string]any(nil),c.rows...) }

func fields() []any { return []any{
    map[string]any{"name":"device_id","type":"utf8","nullable":false},
    map[string]any{"name":"v","type":"int64","nullable":false},
} }
func table(threshold int) map[string]any { return map[string]any{
    "fields":[]any{map[string]any{"name":"device_id","type":"utf8","nullable":false},map[string]any{"name":"threshold","type":"int64","nullable":false}},
    "keys":[]string{"device_id"},"rows":[]any{[]any{"a",threshold},[]any{"b",threshold+10}},
} }
func spec(file, sink string, revision map[string]any) map[string]any { return map[string]any{
    "version":1,"stream":"sensors","reference_tables":map[string]any{"limits":map[string]any{"revision":revision["revision"],"sha256":revision["sha256"]}},
    "source":map[string]any{"kind":"file","path":file,"file_contract":"append_only","inbox_capacity":8},
    "sink":map[string]any{"kind":"http","url":sink,"batch_rows":8,"linger_ms":1,"outbox_capacity":8},
    "delivery":"live_best_effort","recovery":"restart_fresh",
    "graph":map[string]any{"version":1,"pipeline_id":8,"revision_id":1,"nodes":[]any{
        map[string]any{"id":1,"kind":"memory_source","table":"sensors","out":[]int{2}},
        map[string]any{"id":2,"kind":"lookup","table":"limits","on":[]any{map[string]any{"stream":"device_id","table":"device_id"}},"keep":[]string{"threshold"},"out":[]int{3}},
        map[string]any{"id":3,"kind":"capture_sink","name":"out"},
    }},
} }
func appendRow(file,key string, value int) {
    f,e:=os.OpenFile(file,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0600); must(e)
    _,e=f.Write(append(encoded(map[string]any{"device_id":key,"v":value}),'\n')); must(e); must(f.Sync()); must(f.Close())
}
func expectRows(c *capture, start int, keys []string, values []int, thresholds []any) {
    wait("independent Lookup output",func() bool { return len(c.snapshot())>=start+len(keys) })
    rows:=c.snapshot(); require(len(rows)==start+len(keys),"unexpected extra Lookup output")
    for i,key:=range keys {
        want:=map[string]any{"device_id":key,"v":values[i],"threshold":thresholds[i]}
        require(bytes.Equal(encoded(rows[start+i]),encoded(want)),fmt.Sprintf("Lookup row %d got=%s want=%s",i,encoded(rows[start+i]),encoded(want)))
    }
}

func nested(value map[string]any, path ...string) any {
    var current any=value
    for _,key:=range path { next,ok:=current.(map[string]any);if !ok{return nil};current=next[key] }
    return current
}
func bindingStatus(a api, storedRevision, actualRevision, storedTable, actualTable int) map[string]any {
    var result map[string]any
    wait("stored/latest and running/actual binding identities",func()bool {
        result=a.ok("GET","/v1/pipelines/lookup/status",nil)
        return nested(result,"actual","status")=="running" &&
            nested(result,"reference_tables","stored_latest","revision")==float64(storedRevision) &&
            nested(result,"reference_tables","stored_latest","bindings","limits","revision")==float64(storedTable) &&
            nested(result,"reference_tables","running_actual","available")==true &&
            nested(result,"reference_tables","running_actual","revision")==float64(actualRevision) &&
            nested(result,"reference_tables","running_actual","bindings","limits","revision")==float64(actualTable)
    })
    return result
}

func main() {
    binary:=flag.String("server-bin","","production Server"); old:=flag.String("old-server-bin","","pre-catalog-v3 Server"); output:=flag.String("out","","new evidence directory"); flag.Parse()
    require(*binary!="" && *output!="","server-bin and out required")
    root,e:=filepath.Abs(*output); must(e); must(os.Mkdir(root,0700))
    self,e:=os.Executable(); must(e); save(filepath.Join(root,"binaries.json"),map[string]any{"server":hash(*binary),"driver":hash(self)})
    captured:=&capture{}; sink:=httptest.NewServer(http.HandlerFunc(captured.handler)); defer sink.Close()
    defer func(){save(filepath.Join(root,"outputs.json"),captured.snapshot())}()
    sinkURL,e:=url.Parse(sink.URL); must(e); _,portText,e:=net.SplitHostPort(sinkURL.Host); must(e); sinkPort,e:=strconv.Atoi(portText); must(e)
    port:=freePort(); catalog:=filepath.Join(root,"catalog.db"); file:=filepath.Join(root,"input.ndjson")
    a:=api{fmt.Sprintf("http://127.0.0.1:%d",port),&http.Client{Timeout:3*time.Second}}
    var server *child
    start:=func(){ server=launch(*binary,root,catalog,filepath.Join(root,"server.log"),port); wait("Server health",func()bool{_,code:=a.call("GET","/v1/health",nil,false);return code==200}) }
    stop:=func(sig os.Signal){server.stop(sig);server=nil}; defer func(){stop(syscall.SIGKILL)}()
    start()
    _,code:=a.call("PUT","/v1/tables/limits",map[string]any{"expected_revision":0,"table":table(10)},false); require(code==401 || code==403,"unauthorized table mutation accepted")
    a.ok("PUT","/v1/allowlist",map[string]any{"host":"127.0.0.1","port":sinkPort})
    a.ok("PUT","/v1/streams/sensors",map[string]any{"fields":fields()})
    r1:=a.ok("PUT","/v1/tables/limits",map[string]any{"expected_revision":0,"table":table(10)}); save(filepath.Join(root,"table-v1.json"),r1)
    a.rejected("PUT","/v1/tables/limits",map[string]any{"expected_revision":0,"table":table(90)})
    bad:=table(90); bad["rows"]=[]any{[]any{"a",1},[]any{"a",2}}
    a.rejected("PUT","/v1/tables/limits",map[string]any{"expected_revision":1,"table":bad})
    require(a.ok("GET","/v1/tables/limits",nil)["sha256"]==r1["sha256"],"failed publication changed head")
    appendRow(file,"a",1); appendRow(file,"unknown",2)
    oldSpec:=spec(file,sink.URL,r1); a.ok("POST","/v1/validate",oldSpec); a.ok("PUT","/v1/pipelines/lookup",oldSpec); a.ok("POST","/v1/pipelines/lookup/start",nil)
    expectRows(captured,0,[]string{"a","unknown"},[]int{1,2},[]any{10,nil})
    r2:=a.ok("PUT","/v1/tables/limits",map[string]any{"expected_revision":1,"table":table(90)})
    appendRow(file,"b",3); expectRows(captured,2,[]string{"b"},[]int{3},[]any{20})
    r3:=a.ok("PUT","/v1/tables/limits",map[string]any{"expected_revision":2,"table":table(30)})
    gc:=a.ok("POST","/v1/tables/limits/gc",map[string]any{}); save(filepath.Join(root,"gc-before-restart.json"),gc)
    dependencies:=a.ok("GET","/v1/tables/limits/dependencies",nil)
    save(filepath.Join(root,"dependencies-before-restart.json"),dependencies)
    pins,ok:=nested(dependencies,"pins","items").([]any)
    require(ok && len(pins)==1 && nested(dependencies,"pins","total_count")==float64(1) &&
        nested(dependencies,"pins","truncated")==false,"dependency preview omitted or miscounted the pipeline pin")
    pin,ok:=pins[0].(map[string]any)
    require(ok && pin["pipeline"]=="lookup" && pin["pipeline_revision"]==float64(1) &&
        pin["table_revision"]==float64(1) && pin["sha256"]==r1["sha256"],"dependency preview returned the wrong identity")
    a.rejected("GET","/v1/tables/limits/revisions/2",nil)
    require(a.ok("GET","/v1/tables/limits/revisions/1",nil)["sha256"]==r1["sha256"],"GC deleted historical pipeline dependency")
    require(r2["sha256"]!=r3["sha256"],"published table identities collided")
    before:=len(captured.snapshot()); stop(syscall.SIGKILL); start()
    a.ok("POST","/v1/pipelines/lookup/start",nil)
    expectRows(captured,before,[]string{"a","unknown","b"},[]int{1,2,3},[]any{10,nil,20})
    save(filepath.Join(root,"old-binding-restart-status.json"),bindingStatus(a,1,1,1,1))
    a.ok("POST","/v1/pipelines/lookup/stop",nil)
    before=len(captured.snapshot()); newSpec:=spec(file,sink.URL,r3)
    a.ok("PUT","/v1/pipelines/lookup",newSpec); a.ok("POST","/v1/pipelines/lookup/start",nil)
    expectRows(captured,before,[]string{"a","unknown","b"},[]int{1,2,3},[]any{30,nil,40})
    save(filepath.Join(root,"new-binding-status.json"),bindingStatus(a,2,2,3,3))
    save(filepath.Join(root,"gc-after-binding-update.json"),a.ok("POST","/v1/tables/limits/gc",map[string]any{}))
    require(a.ok("GET","/v1/tables/limits/revisions/1",nil)["sha256"]==r1["sha256"],"GC deleted old startable pipeline revision dependency")
    a.ok("POST","/v1/pipelines/lookup/stop",nil)
    before=len(captured.snapshot()); a.ok("POST","/v1/pipelines/lookup/start",map[string]any{"revision":1})
    expectRows(captured,before,[]string{"a","unknown","b"},[]int{1,2,3},[]any{10,nil,20})
    save(filepath.Join(root,"historical-binding-status.json"),bindingStatus(a,2,1,3,1))
    // Time-state recovery requires its separate time contract. Keep this
    // negative test meaningful when Lookup -> Count is admitted.
    rejected:=spec(file,sink.URL,r1); rejected["recovery"]="aligned"
    rejected["checkpoint_dir"]=filepath.Join(root,"unsupported-lookup-count")
    graph:=rejected["graph"].(map[string]any); nodes:=graph["nodes"].([]any)
    nodes[1].(map[string]any)["out"]=[]int{4}
    graph["nodes"]=[]any{nodes[0],nodes[1],map[string]any{"id":4,"kind":"window_agg","keys":[]string{"device_id"},"window":map[string]any{"kind":"tumble_pt","size_micros":1000000},"aggs":[]any{map[string]any{"fn":"sum","expr":map[string]any{"k":"col","name":"v"},"alias":"total"}},"out":[]int{3}},nodes[2]}
    a.rejected("POST","/v1/validate",rejected)
    stop(syscall.SIGTERM)
    checkedOld:=false
    if *old!="" {
        oldRoot:=filepath.Join(root,"old-catalog-guard"); must(os.Mkdir(oldRoot,0700))
        raw,e:=os.ReadFile(catalog);must(e);copyCatalog:=filepath.Join(oldRoot,"catalog.db");must(os.WriteFile(copyCatalog,raw,0600)); initialHash:=hash(copyCatalog)
        legacy:=launch(*old,oldRoot,copyCatalog,filepath.Join(oldRoot,"server.log"),freePort())
        exited:=false
        select {case err:=<-legacy.done:require(err!=nil,"old binary accepted new catalog");exited=true;case <-time.After(8*time.Second):}
        if exited {must(legacy.log.Close());legacy.cmd=nil}else{legacy.stop(syscall.SIGKILL)}
        require(exited,"old binary did not reject catalog v3")
        log,e:=os.ReadFile(filepath.Join(oldRoot,"server.log"));must(e)
        require(bytes.Contains(log,[]byte("catalog_schema_version 3")) && bytes.Contains(log,[]byte("not supported")),"old binary failed for a reason other than catalog v3 guard")
        require(hash(copyCatalog)==initialHash,"old binary modified protected catalog")
        checkedOld=true
    }
    save(filepath.Join(root,"summary.json"),map[string]any{
        "valid":true,"profile":"managed_static_lookup_restart_fresh","publication_cas":true,"invalid_publication_atomic":true,
        "hit_and_miss_golden":true,"running_binding_immutable":true,"gc_preserved_all_pipeline_revisions":true,
        "unreferenced_revision_deleted":true,"sigkill_reloaded_original_binding":true,"explicit_binding_update":true,
        "historical_pipeline_start":true,"aligned_rejected":true,"old_catalog_guard":checkedOld,"certified":false,
    })
    fmt.Println("CORE_B_PROCESS_OK immutable static table revisions, exact binding, GC, restart and historical start")
}
