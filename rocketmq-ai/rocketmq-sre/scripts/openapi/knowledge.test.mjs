// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import {extendKnowledge} from "./knowledge.mjs";

const readSchema=(filename)=>JSON.parse(fs.readFileSync(new URL(`../../schemas/${filename}`,import.meta.url),"utf8"));
const fresh=()=>({paths:Object.fromEntries([
  ["/v1/knowledge","get"],["/v1/knowledge/search","get"],["/v1/knowledge/{id}","get"],
  ["/v1/knowledge/import","post"],["/v1/knowledge/{id}/review","post"],["/v1/knowledge/{id}/feedback","post"],
].map(([path,method])=>[path,{[method]:{operationId:path,responses:{403:{description:"existing authorization refusal"}}}}])),
components:{schemas:{ErrorEnvelope:{type:"object"},Unrelated:{const:"preserved"}}}});

test("Knowledge replaces generic responses with real Rust fields and adds only bounded history",()=>{
  const document=fresh();const before=structuredClone(document);
  extendKnowledge({document,readSchema});
  assert.equal(Object.keys(document.paths).length,Object.keys(before.paths).length+1);
  assert.equal(document.paths["/v1/knowledge/{id}/history"].get.operationId,"getKnowledgeHistory");
  assert(!Object.hasOwn(document.components.schemas.KnowledgeItem.properties,"summary"));
  for(const key of ["binding","bounds","observed_at"])assert(Object.hasOwn(document.components.schemas.KnowledgeSearchPage.properties,key));
  assert.deepEqual(document.components.schemas.Unrelated,before.components.schemas.Unrelated);
  for(const [path,methods]of Object.entries(before.paths))for(const method of Object.keys(methods)) {
    assert.equal(document.paths[path][method].operationId,methods[method].operationId);
    assert.deepEqual(document.paths[path][method].responses[403],methods[method].responses[403]);
  }
});

test("Knowledge references resolve locally and normal regeneration is stable",()=>{
  const document=fresh();extendKnowledge({document,readSchema});
  const first=JSON.stringify(document);extendKnowledge({document,readSchema});assert.equal(JSON.stringify(document),first);
  const visit=(value)=>{
    if(Array.isArray(value))return value.forEach(visit);
    if(!value||typeof value!=="object")return;
    if(value.$ref){assert(value.$ref.startsWith("#/components/schemas/"));assert(Object.hasOwn(document.components.schemas,value.$ref.slice(21)),value.$ref);}
    Object.values(value).forEach(visit);
  };
  visit(document);
  assert.throws(()=>extendKnowledge({document:fresh(),readSchema:()=>({$ref:"https://invalid.example/schema"})}),/non-local/);
});
