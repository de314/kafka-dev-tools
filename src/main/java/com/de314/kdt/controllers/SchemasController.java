package com.de314.kdt.controllers;

import com.de314.kdt.models.SchemaInfoModel;
import com.de314.kdt.models.SchemaRegistryRestException;
import com.de314.kdt.models.SchemaVersionModel;
import com.de314.kdt.services.SchemaRegistryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/schemas")
public class SchemasController {

    private final SchemaRegistryService schemaRegistryService;

    public SchemasController(SchemaRegistryService schemaRegistryService) {
        this.schemaRegistryService = schemaRegistryService;
    }

    @GetMapping
    public ResponseEntity<List<String>> schemas(@RequestParam("env") String kEnvId,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.findAll(kEnvId, oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @GetMapping("/{name}")
    public ResponseEntity<SchemaInfoModel> info(@PathVariable("name") String name,
            @RequestParam("env") String kEnvId,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getInfo(name, kEnvId, oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }

    @GetMapping("/{name}/{version}")
    public ResponseEntity<SchemaVersionModel> version(@PathVariable("name") String name,
            @PathVariable("version") Integer version,
            @RequestParam("env") String kEnvId,
            @RequestParam("skipCache") Optional<Boolean> oSkipCache) {
        try {
            return ResponseEntity.ok(schemaRegistryService.getVersion(name, version, kEnvId, oSkipCache.orElse(false)));
        } catch (SchemaRegistryRestException e) {
            return ResponseEntity.status(e.getStatusCode())
                    .header("error-message", e.getMessage())
                    .body(null);
        }
    }
}
