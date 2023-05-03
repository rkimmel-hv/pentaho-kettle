/*!
 * Copyright 2010 - 2022   Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.pentaho.metastore.locator.api.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.pentaho.di.core.attributes.metastore.JsonElement;
import org.pentaho.di.core.attributes.metastore.JsonElementType;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.kettle.repository.locator.api.KettleRepositoryProvider;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.IMetaStoreElement;
import org.pentaho.metastore.api.IMetaStoreElementType;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.pentaho.metastore.locator.api.MetastoreProvider;
import org.pentaho.metastore.locator.api.impl.MetastoreLocatorImpl;

import com.google.common.collect.ImmutableMap;
import org.pentaho.metastore.stores.memory.MemoryMetaStore;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.di.core.util.Assert.assertNotNull;

/**
 * Created by tkafalas 7/26/2017.
 */
public class MetastoreLocatorImplTest {
  private MetastoreLocatorImpl metastoreLocator;

  @Before
  public void setup() {
    metastoreLocator = new MetastoreLocatorImpl();
  }

  @Test
  public void testgetMetastoreNone() {
    assertNull( metastoreLocator.getExplicitMetastore( "" ) );
  }

  @Test
  public void testGetMetastoreSingleNull() {
    // Test a null metastore provider that delivers a null metastore
    MetastoreProvider provider = mock( MetastoreProvider.class );
    when( provider.getProviderType() ).thenReturn( MetastoreLocator.LOCAL_PROVIDER_KEY );
    Collection<MetastoreProvider> providerCollection = new ArrayList<>();
    providerCollection.add( provider );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreProvider.class ) )
        .thenReturn( providerCollection );

      assertNull( metastoreLocator.getExplicitMetastore( MetastoreLocator.LOCAL_PROVIDER_KEY ) );
      verify( provider ).getMetastore();
    }
  }

  @Test
  public void testGetMetastoreTest() {
    //Test that repository metastore gets returned if both local and repository metastore providers exist.
    //Also test that both providers can be accessed directly.
    MetastoreProvider localProvider = mock( MetastoreProvider.class );
    IMetaStore localMeta = mock( IMetaStore.class );
    when( localProvider.getMetastore() ).thenReturn( localMeta );
    when( localProvider.getProviderType() ).thenReturn( MetastoreLocator.LOCAL_PROVIDER_KEY );
    MetastoreProvider repoProvider = mock( MetastoreProvider.class );
    IMetaStore repoMeta = mock( IMetaStore.class );
    when( repoProvider.getMetastore() ).thenReturn( repoMeta );
    when( repoProvider.getProviderType() ).thenReturn( MetastoreLocator.REPOSITORY_PROVIDER_KEY );
    Collection<MetastoreProvider> providerCollection = new ArrayList<>();
    providerCollection.add( localProvider );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreProvider.class ) )
        .thenReturn( providerCollection );

      // only local provider exists
      assertEquals( localMeta, metastoreLocator.getMetastore() );
      providerCollection.clear();
      providerCollection.add( repoProvider );
      // only repo provider exists
      assertEquals( repoMeta, metastoreLocator.getMetastore() );
      providerCollection.add( localProvider );

      // both providers exist
      assertEquals( localMeta, metastoreLocator.getExplicitMetastore( MetastoreLocator.LOCAL_PROVIDER_KEY ) );
      assertEquals( repoMeta, metastoreLocator.getExplicitMetastore( MetastoreLocator.REPOSITORY_PROVIDER_KEY ) );
    }
  }

  @Test
  public void testSetAndDisposeEmbeddedMetastore() throws MetaStoreException {
    IMetaStore embeddedMeta = mock( IMetaStore.class );
    when( embeddedMeta.getName() ).thenReturn( "MetastoreUniqueName" );
    String key = metastoreLocator.setEmbeddedMetastore( embeddedMeta );
    assertEquals( "MetastoreUniqueName", key );
    assertNotNull( key, "Embedded key value not returned" );
    assertEquals( embeddedMeta, metastoreLocator.getExplicitMetastore( key ) );
    assertEquals( embeddedMeta, metastoreLocator.getMetastore( key ) );

    metastoreLocator.disposeMetastoreProvider( key );
    assertNull( metastoreLocator.getExplicitMetastore( key ) );
  }

  @Test
  public void testExportMetastoreData() throws MetaStoreException {
    // Test a null metastore provider that delivers a null metastore
    MetastoreProvider provider = mock( MetastoreProvider.class );
    when( provider.getProviderType() ).thenReturn( MetastoreLocator.LOCAL_PROVIDER_KEY );

    MemoryMetaStore metaStore = new MemoryMetaStore();
    String namespace = "foobar";
    metaStore.createNamespace( namespace );
    IMetaStoreElementType elementType = metaStore.newElementType( namespace );
    elementType.setName( "foobar_element_type" );
    elementType.setDescription( "foobar_element_type_description" );
    metaStore.createElementType( namespace, elementType );

    IMetaStoreElement element = metaStore.newElement();
    element.setName( "Test Element" );
    element.setElementType( elementType );

    element.addChild( metaStore.newAttribute( "foo", true ) );
    element.addChild( metaStore.newAttribute( "bar", 1.23456789 ) );
    element.addChild( metaStore.newAttribute( "baz", null ) );
    element.addChild( metaStore.newAttribute( "Dagon", "fish" ) );
    element.addChild( metaStore.newAttribute( "Nyarlathotep", new Object() ) );
    element.addChild( metaStore.newAttribute( "necronomicon", "book" ) );

    metaStore.createElement( namespace, elementType, element );

    when( provider.getMetastore() ).thenReturn( metaStore );
    Collection<MetastoreProvider> providerCollection = new ArrayList<>();
    providerCollection.add( provider );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreProvider.class ) )
        .thenReturn( providerCollection );

      assertEquals( metaStore, metastoreLocator.getExplicitMetastore( MetastoreLocator.LOCAL_PROVIDER_KEY ) );
      verify( provider ).getMetastore();

      Map<String, List<IMetaStoreElement>> metastoreData = new HashMap<>();
      List<String> namespaces = metaStore.getNamespaces();
      for ( String metaStoreNamespace
        :namespaces ) {

        List<IMetaStoreElementType> types = metaStore.getElementTypes( metaStoreNamespace );
        for ( IMetaStoreElementType type
          :types ) {

          List<IMetaStoreElement> elements = metaStore.getElements( metaStoreNamespace, type );
          metastoreData.put( metaStoreNamespace, elements );
        }
      }

      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      String metastoreDataJson = gson.toJson( metastoreData );
      System.out.println( metastoreDataJson );
      assertEquals( "{\n"
        + "  \"foobar\": [\n"
        + "    {\n"
        + "      \"name\": \"Test Element\",\n"
        + "      \"elementType\": {\n"
        + "        \"elementMap\": {},\n"
        + "        \"readLock\": {\n"
        + "          \"sync\": {\n"
        + "            \"state\": 0\n"
        + "          }\n"
        + "        },\n"
        + "        \"writeLock\": {\n"
        + "          \"sync\": {\n"
        + "            \"state\": 0\n"
        + "          }\n"
        + "        },\n"
        + "        \"namespace\": \"foobar\",\n"
        + "        \"id\": \"foobar_element_type\",\n"
        + "        \"name\": \"foobar_element_type\",\n"
        + "        \"description\": \"foobar_element_type_description\"\n"
        + "      },\n"
        + "      \"ownerPermissionsList\": [],\n"
        + "      \"readLock\": {\n"
        + "        \"sync\": {\n"
        + "          \"state\": 0\n"
        + "        }\n"
        + "      },\n"
        + "      \"writeLock\": {\n"
        + "        \"sync\": {\n"
        + "          \"state\": 0\n"
        + "        }\n"
        + "      },\n"
        + "      \"children\": {\n"
        + "        \"bar\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"bar\",\n"
        + "          \"value\": 1.23456789\n"
        + "        },\n"
        + "        \"Dagon\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"Dagon\",\n"
        + "          \"value\": \"fish\"\n"
        + "        },\n"
        + "        \"Nyarlathotep\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"Nyarlathotep\",\n"
        + "          \"value\": {}\n"
        + "        },\n"
        + "        \"necronomicon\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"necronomicon\",\n"
        + "          \"value\": \"book\"\n"
        + "        },\n"
        + "        \"foo\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"foo\",\n"
        + "          \"value\": true\n"
        + "        },\n"
        + "        \"baz\": {\n"
        + "          \"readLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"writeLock\": {\n"
        + "            \"sync\": {\n"
        + "              \"state\": 0\n"
        + "            }\n"
        + "          },\n"
        + "          \"children\": {},\n"
        + "          \"id\": \"baz\"\n"
        + "        }\n"
        + "      },\n"
        + "      \"id\": \"Test Element\"\n"
        + "    }\n"
        + "  ]\n"
        + "}", metastoreDataJson );

    }
  }
}
