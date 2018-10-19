package com.de314.kdt.services;

import com.de314.kdt.models.Page;

/**
 * Created by davidesposito on 7/21/16.
 */
public interface RegistryService<ModelT> {

    void register(ModelT model);

    Page<ModelT> findAll();

    ModelT findById(String id);

    ModelT remove(String id);
}
