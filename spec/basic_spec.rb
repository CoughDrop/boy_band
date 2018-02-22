require 'spec_helper'
require 'permissable'
require 'active_support'

describe Permissable do
  module Permissions
    extend ActiveSupport::Concern
    include Permissable::InstanceMethods
    
    module ClassMethods
      include Permissable::ClassMethods
    end
  end
  
  class PermitObject
    include Permissions
    def id
      12345
    end
    
    def updated_at
      Time.parse("Jun 1, 2016")
    end
  end
  
  class PermitObject2
    include Permissions
  end
  
  before(:each) do
    PermitObject.permissions_lookup = []
    PermitObject2.permissions_lookup = []
    PermitObject.allow_cached_permissions = false
  end
  
  it "should have not error on init" do
    expect(PermitObject.new).to_not eq(nil)
  end

  describe "FakeRedis" do
    it "should have default redis behavior" do
      expect(Permissable.permissions_redis).to_not eq(nil)
      expect(Permissable.permissions_redis.get('asdf')).to eq(nil)
      expect(Permissable.permissions_redis.setex('asdf')).to eq(false)
      expect(Permissable.permissions_redis.del('asdf')).to eq(false)
      expect(Permissable.cache_token).to eq('permissable_redis_cache_token')
    end
  end
  
  it "should return a default cache key" do
    expect(PermitObject.new.cache_key).to eq('PermitObject12345-1464760800.0:permissable_redis_cache_token')
    expect(PermitObject.new.cache_key('bacon')).to eq('bacon/PermitObject12345-1464760800.0:permissable_redis_cache_token')
  end
  
  describe "set_cached" do
    it "should set the right expiration, prefix, and json value" do
      data = {a: 1, b: 2}
      expect(Permissable.permissions_redis).to receive(:setex).with('bacon/PermitObject12345-1464760800.0:permissable_redis_cache_token', 1800, data.to_json).and_return(true)
      PermitObject.new.set_cached('bacon', data)
    end
    
    it "should use the specified expiration if defined" do
      data = {a: 1, b: 2}
      expect(Permissable.permissions_redis).to receive(:setex).with('bacon/PermitObject12345-1464760800.0:permissable_redis_cache_token', 12345, data.to_json).and_return(true)
      PermitObject.new.set_cached('bacon', data, 12345)
    end
  end
  
  describe "get_cached" do
    it "should return nil on cache miss" do
      expect(Permissable.permissions_redis).to receive(:get).with('asdf/PermitObject12345-1464760800.0:permissable_redis_cache_token').and_return(nil)
      expect(PermitObject.new.get_cached('asdf')).to eq(nil)
    end
    
    it "should return parsed json on cache hit" do
      expect(Permissable.permissions_redis).to receive(:get).with('asdf/PermitObject12345-1464760800.0:permissable_redis_cache_token').and_return({a: 1, b: 2}.to_json)
      expect(PermitObject.new.get_cached('asdf')).to eq({'a' => 1, 'b' => 2})
    end 
    
    it "should return nil on malformed json cache hit" do
      expect(Permissable.permissions_redis).to receive(:get).with('asdf/PermitObject12345-1464760800.0:permissable_redis_cache_token').and_return('this is not valid json')
      expect(PermitObject.new.get_cached('asdf')).to eq(nil)
    end
  end
  
  describe "clear_cached" do
    it "should call del on redis instance" do
      expect(Permissable.permissions_redis).to receive(:del).with('something/PermitObject12345-1464760800.0:permissable_redis_cache_token')
      expect(PermitObject.new.clear_cached('something')).to eq(nil)
    end
  end
  
  describe "cache_permissions" do
    it "should enable cached permissions" do
      PermitObject.cache_permissions
      expect(PermitObject.allow_cached_permissions).to eq(true)
    end
  end
  
  describe "add_permissions" do
    it "should add permissions to the correct object with the default scope" do
      PermitObject.add_permissions('hat') { true }
      expect(PermitObject.permissions_lookup).to_not eq(nil)
      expect(PermitObject.permissions_lookup.length).to eq(1)
      expect(PermitObject.permissions_lookup[0][0]).to eq(['hat'])
      expect(PermitObject.permissions_lookup[0][2]).to eq(['full'])
      expect(PermitObject2.permissions_lookup).to eq([])
    end

    it "should add star permissions to the correct object" do
      PermitObject.add_permissions('cat', ['*']) { true }
      expect(PermitObject.permissions_lookup).to_not eq(nil)
      expect(PermitObject.permissions_lookup.length).to eq(1)
      expect(PermitObject.permissions_lookup[0][0]).to eq(['cat'])
      expect(PermitObject.permissions_lookup[0][2]).to eq(['*', 'full'])
      expect(PermitObject2.permissions_lookup).to eq([])
    end
  end
  
  describe "allows?" do
    it "should return correct permissions" do
      PermitObject.add_permissions('cat') { true }
      PermitObject.add_permissions('frog') { false }
      PermitObject.add_permissions('frog') { true }
      obj = PermitObject.new
      expect(obj.allows?(nil, 'frog')).to eq(true)
      expect(obj.allows?(nil, 'cat')).to eq(true)
      expect(obj.allows?(nil, 'something')).to eq(false)
    end
    
    it "should short-circuit once a matching permission is met" do
      PermitObject.add_permissions('cat') { true }
      PermitObject.add_permissions('frog', 'horse') { true }
      PermitObject.add_permissions('horse') { raise 'asdf' }
      PermitObject.add_permissions('bacon') { raise 'jkl' }
      obj = PermitObject.new
      expect(obj.allows?(nil, 'frog')).to eq(true)
      expect(obj.allows?(nil, 'cat')).to eq(true)
      expect(obj.allows?(nil, 'horse')).to eq(true)
      expect{obj.allows?(nil, 'bacon')}.to raise_error('jkl')
    end
    
    it "should not call checks requiring a user if no user is provided" do
      PermitObject.add_permissions('cat') {|u| raise "no" }
      PermitObject.add_permissions('frog', 'horse') { true }
      PermitObject.add_permissions('horse') { raise 'asdf' }
      obj = PermitObject.new
      expect(obj.allows?(nil, 'frog')).to eq(true)
      expect(obj.allows?(nil, 'cat')).to eq(false)
      expect(obj.allows?(nil, 'horse')).to eq(true)
    end

    it "should correctly check user permissions" do
      PermitObject.add_permissions('cat') {|u| u[:a] == 1 }
      obj = PermitObject.new
      expect(obj.allows?({a: 1}, 'cat')).to eq(true)
      expect(obj.allows?({a: 2}, 'cat')).to eq(false)
    end
    
    it "should flag as scope-rejected if true" do
      PermitObject.add_permissions('frog', 'horse') { true }
      obj = PermitObject.new
      action = 'frog'
      res = obj.allows?(nil, action, [])
      expect(res).to eq(false)
      expect(action.instance_variable_get('@scope_rejected')).to eq(true)
    end
    
    it "should call permissions_for if allowing cached permissions" do
      PermitObject.cache_permissions
      obj = PermitObject.new
      user = {}
      expect(obj).to receive(:permissions_for).with(user, ['a', '*']).and_return({
        'hat' => true,
        'cat' => false
      }).exactly(2).times
      action = 'cat'
      expect(obj.allows?(user, action, ['a'])).to eq(false)
      expect(action.instance_variable_get('@scope_rejected')).to eq(true)
      action = 'hat'
      expect(obj.allows?(user, action, ['a'])).to eq(true)
      expect(action.instance_variable_get('@scope_rejected')).to eq(false)
    end
  end
  
  describe "permissions_for" do
    it "should return a list of permissions" do
      PermitObject.add_permissions('jump') { true }
      PermitObject.add_permissions('swing') { false }
      PermitObject.add_permissions('fly') { true }
      obj = PermitObject.new
      expect(obj.permissions_for(nil)).to eq({
        'user_id' => nil,
        'jump' => true,
        'fly' => true
      })
    end
    
    it "should not call checks whose permissions have all already been granted" do
      PermitObject.add_permissions('jump') { true }
      PermitObject.add_permissions('swing') { true }
      PermitObject.add_permissions('fly', 'jump') { true }
      PermitObject.add_permissions('swing', 'jump') { raise 'asdf' }
      obj = PermitObject.new
      expect(obj.permissions_for(nil)).to eq({
        'user_id' => nil,
        'jump' => true,
        'fly' => true,
        'swing' => true
      })
    end
    
    it "should not call checks that require a user if no user is set" do
      PermitObject.add_permissions('jump') { true }
      PermitObject.add_permissions('fly', 'jump') { true }
      PermitObject.add_permissions('swing', 'jump') {|u| raise 'asdf' }
      obj = PermitObject.new
      expect(obj.permissions_for(nil)).to eq({
        'user_id' => nil,
        'jump' => true,
        'fly' => true
      })
    end
    
    it "should set scope-rejected permissions to false, not leave as nil" do
      PermitObject.add_permissions('jump', ['*']) { true }
      PermitObject.add_permissions('fly', 'jump') { true }
      obj = PermitObject.new
      expect(obj.permissions_for(nil, [])).to eq({
        'user_id' => nil,
        'jump' => true,
        'fly' => false
      })
    end
    
    it "should persist a cache of the permissions if enabled" do
      PermitObject.cache_permissions
      PermitObject.add_permissions('jump', ['*']) { true }
      PermitObject.add_permissions('fly', 'jump') { true }
      obj = PermitObject.new
      expect(obj).to receive(:set_cached).with("permissions-for/nobody/scopes_*", {"user_id"=>nil, "jump"=>true, "fly"=>false})
      expect(obj.permissions_for(nil, [])).to eq({
        'user_id' => nil,
        'jump' => true,
        'fly' => false
      })
    end
    
    it "should retrieve permissions from the cache if enabled" do
      PermitObject.cache_permissions
      obj = PermitObject.new
      user = OpenStruct.new(cache_key: 'asdf', global_id: 'asdf')
      expect(obj).to receive(:get_cached).with('permissions-for/nobody/scopes_a,*').and_return({a: 1})
      expect(obj).to receive(:get_cached).with('permissions-for/asdf/scopes_full,*').and_return({b: 1})
      expect(obj.permissions_for(nil, ['a'])).to eq({a: 1})
      expect(obj.permissions_for(user)).to eq({b: 1})
    end
  end
end
