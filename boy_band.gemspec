Gem::Specification.new do |s|
  s.name        = 'boy_band'

  s.add_development_dependency 'rails'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'ruby-debug'

  s.version     = '0.1.10'
  s.date        = '2020-02-25'
  s.summary     = "BoyBand"
  s.extra_rdoc_files = %W(LICENSE)
  s.homepage = %q{http://github.com/CoughDrop/boy_band}
  s.description = "Async/Background helper gem, used by multiple CoughDrop libraries"
  s.authors     = ["Brian Whitmer"]
  s.email       = 'brian.whitmer@gmail.com'

	s.files = Dir["{lib}/**/*"] + ["LICENSE", "README.md"]
  s.require_paths = %W(lib)

  s.license     = 'MIT'
end