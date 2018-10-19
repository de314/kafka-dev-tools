package com.de314.kdt.controllers;

import com.de314.kdt.models.SchemaInfoModel;
import com.de314.kdt.models.SchemaRegistryRestException;
import com.de314.kdt.models.SchemaVersionModel;
import com.de314.kdt.services.SchemaRegistryService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/schemas")
public class CachedSchemaRegistryProxy {

    private final SchemaRegistryService schemaRegistryService;

    public CachedSchemaRegistryProxy(SchemaRegistryService schemaRegistryService) {
        this.schemaRegistryService = schemaRegistryService;
    }

    @GetMapping
    public ResponseEntity<List<String>> schemas(@RequestParam("url") Optional<String> oUrl,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.findAll(oUrl.orElse(null), oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @GetMapping("/{name}")
    public ResponseEntity<SchemaInfoModel> info(@PathVariable("name") String name,
            @RequestParam("url") Optional<String> oUrl,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getInfo(name, oUrl.orElse(null), oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @GetMapping("/{name}/{version}")
    public ResponseEntity<SchemaVersionModel> version(@PathVariable("name") String name,
            @PathVariable("version") Integer version,
            @RequestParam("url") Optional<String> oUrl,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getVersion(name, version, oUrl.orElse(null), oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }
}
