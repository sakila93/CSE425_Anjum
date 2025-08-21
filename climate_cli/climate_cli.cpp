#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using std::cin;
using std::cout;
using std::endl;
using std::flush;
using std::function;
using std::ifstream;
using std::numeric_limits;
using std::pair;
using std::size_t;
using std::string;
using std::unordered_map;
using std::vector;

static inline string ltrim(string s){ size_t i=0; while(i<s.size() && std::isspace((unsigned char)s[i])) ++i; return s.substr(i); }
static inline string rtrim(string s){ size_t j=s.size(); while(j>0 && std::isspace((unsigned char)s[j-1])) --j; return s.substr(0,j); }
static inline string trim(string s){ return rtrim(ltrim(s)); }
static inline string lower(string s){ for(char &c:s) c=(char)std::tolower((unsigned char)c); return s; }
static inline bool is_missing(const string& s){ string t=lower(trim(s)); return t.empty()||t=="na"||t=="n/a"||t=="null"||t=="nan"; }

static bool parse_int(const string& s, int& out){
    if (is_missing(s)) { out = std::numeric_limits<int>::min(); return true; }
    try { size_t p=0; long long v = std::stoll(s,&p); if(p==0) return false; out=(int)v; return true; } catch(...){ out=std::numeric_limits<int>::min(); return false; }
}
static bool parse_double(const string& s, double& out){
    if (is_missing(s)) { out = std::numeric_limits<double>::quiet_NaN(); return true; }
    try { size_t p=0; double v = std::stod(s,&p); if(p==0) return false; out=v; return true; } catch(...){ out=std::numeric_limits<double>::quiet_NaN(); return false; }
}

static vector<string> parse_csv_line(const string& line){
    vector<string> f; f.reserve(16);
    string cur; bool inq=false;
    for(size_t i=0;i<line.size();++i){
        char c=line[i];
        if(c=='"'){
            if(inq && i+1<line.size() && line[i+1]=='"'){ cur.push_back('"'); ++i; }
            else inq=!inq;
        }else if(c==',' && !inq){
            f.push_back(cur); cur.clear();
        }else cur.push_back(c);
    }
    f.push_back(cur);
    return f;
}

struct Record{
    string country;
    int    year;
    double temp_anom;             
    double co2;                   
    double gdp;                   
    int    extreme_events;       
};

struct Dataset{
    vector<Record> rows;
    unordered_map<string, vector<size_t>> byCountry;
    unordered_map<int, vector<size_t>>    byYear;
    vector<string> countryNames;

    void index(){
        byCountry.clear(); byYear.clear(); countryNames.clear();
        byCountry.reserve(512);
        byYear.reserve(256);
        for(size_t i=0;i<rows.size();++i){
            byCountry[rows[i].country].push_back(i);
            if(rows[i].year!=std::numeric_limits<int>::min())
                byYear[rows[i].year].push_back(i);
        }
        countryNames.reserve(byCountry.size());
        for (auto& kv: byCountry) countryNames.push_back(kv.first);

        auto cmpYear = [&](size_t a, size_t b){
            int ya = rows[a].year, yb = rows[b].year;
            if (ya==std::numeric_limits<int>::min()) return false;
            if (yb==std::numeric_limits<int>::min()) return true;
            return ya < yb;
        };
        for (auto& kv: byCountry){
            auto &v = kv.second;
            if (v.empty()) continue;
            vector<size_t> tmp(v.size());
            for (size_t width=1; width < v.size(); width*=2){
                for (size_t i=0; i < v.size(); i += 2*width){
                    size_t left=i, mid=std::min(i+width, v.size()), right=std::min(i+2*width, v.size());
                    size_t a=left, b=mid, t=left;
                    while(a<mid && b<right) tmp[t++] = cmpYear(v[a],v[b]) ? v[a++] : v[b++];
                    while(a<mid)  tmp[t++] = v[a++];
                    while(b<right)tmp[t++] = v[b++];
                    for(size_t k=left;k<right;++k) v[k]=tmp[k];
                }
            }
        }
    }
};

struct ColMap { int cCountry=-1, cYear=-1, cTemp=-1, cCO2=-1, cGDP=-1, cExt=-1; };

static int find_col(const vector<string>& hdr, const vector<string>& exacts, const vector<string>& contains){
    for(int i=0;i<(int)hdr.size();++i){
        string t=lower(trim(hdr[i]));
        for(auto &e:exacts) if(t==lower(e)) return i;
    }
    for(int i=0;i<(int)hdr.size();++i){
        string t=lower(trim(hdr[i]));
        for(auto &c:contains) if(t.find(lower(c))!=string::npos) return i;
    }
    return -1;
}

static ColMap detect_columns(const vector<string>& hdr){
    ColMap m;
    m.cCountry = find_col(hdr, {"country","country_name","entity","nation"}, {"country"});
    m.cYear    = find_col(hdr, {"year"}, {"year"});
    m.cTemp    = find_col(hdr, {"temperature_anomaly","temp_anomaly","temperature anomaly"}, {"anomaly","temp"});
    m.cCO2     = find_col(hdr, {"co2","co2_emissions","co2 emissions","total co2 emissions"}, {"co2"});
    m.cGDP     = find_col(hdr, {"gdp","gdp (current us$)","gdp_current_usd"}, {"gdp"});
    m.cExt     = find_col(hdr, {"extreme_weather_events","extreme events","extreme weather events","natural_disasters","disaster_count"}, {"extreme","disaster"});
    return m;
}

static bool load_csv(const string& path, Dataset& ds){
    ifstream in(path);
    if(!in){ std::cerr<<"ERROR: cannot open file: "<<path<<"\n"; return false; }
    string header;
    if(!std::getline(in, header)){ std::cerr<<"ERROR: file is empty\n"; return false; }
    auto hdr = parse_csv_line(header);
    ColMap cm = detect_columns(hdr);
    if(cm.cCountry<0 || cm.cYear<0){
        std::cerr<<"ERROR: could not detect Country/Year headers. Detected: "
                 <<"country="<<cm.cCountry<<" year="<<cm.cYear<<" temp="<<cm.cTemp
                 <<" co2="<<cm.cCO2<<" gdp="<<cm.cGDP<<" extreme="<<cm.cExt<<"\n";
        return false;
    }
    ds.rows.clear();
    ds.rows.reserve(120000);

    string line;
    while(std::getline(in, line)){
        if(line.empty()) continue;
        auto f = parse_csv_line(line);
        auto get = [&](int idx)->string{ return (idx>=0 && idx<(int)f.size())? f[idx] : ""; };

        Record r;
        r.country = trim(get(cm.cCountry));
        parse_int(get(cm.cYear), r.year);

        double d;
        r.temp_anom = (cm.cTemp>=0 && parse_double(get(cm.cTemp), d)) ? d : numeric_limits<double>::quiet_NaN();
        r.co2       = (cm.cCO2>=0 && parse_double(get(cm.cCO2), d))   ? d : numeric_limits<double>::quiet_NaN();
        r.gdp       = (cm.cGDP>=0 && parse_double(get(cm.cGDP), d))   ? d : numeric_limits<double>::quiet_NaN();

        int ev;
        r.extreme_events = (cm.cExt>=0 && parse_int(get(cm.cExt), ev)) ? ev : std::numeric_limits<int>::min();

        if(!r.country.empty()) ds.rows.push_back(r);
    }
    ds.index();
    cout << "Loaded " << ds.rows.size() << " rows.\n" << flush;
    cout << "Countries detected: " << ds.byCountry.size() << "\n" << flush;
    return true;
}


template<class Less>
static void quicksort_index(std::vector<std::size_t>& a, Less less){
    if (a.empty()) return;

    std::vector< std::pair<int,int> > st;
    st.reserve(64);
    st.push_back(std::make_pair(0, (int)a.size()-1));

    while (!st.empty()) {
        std::pair<int,int> fr = st.back();
        st.pop_back();
        int l = fr.first;
        int r = fr.second;

        int i = l, j = r;
        std::size_t pivot = a[(l + r) / 2];

        while (i <= j) {
            while (less(a[i], pivot)) ++i;
            while (less(pivot, a[j])) --j;
            if (i <= j) { std::swap(a[i], a[j]); ++i; --j; }
        }
        if (l < j) st.push_back(std::make_pair(l, j));
        if (i < r) st.push_back(std::make_pair(i, r));
    }
}

static void print_rec(const Record& r){
    cout << r.country
         << " | Year " << (r.year==std::numeric_limits<int>::min()? -9999 : r.year)
         << " | TempAnom=" << (std::isnan(r.temp_anom)? NAN : r.temp_anom)
         << " | CO2=" << (std::isnan(r.co2)? NAN : r.co2)
         << " | GDP=" << (std::isnan(r.gdp)? NAN : r.gdp)
         << " | ExtremeEv=" << (r.extreme_events==std::numeric_limits<int>::min()? -1 : r.extreme_events)
         << "\n";
}

static void search_country(const Dataset& ds, const string& country){
    auto it = ds.byCountry.find(country);
    if(it==ds.byCountry.end()){
    
        string lc = lower(country);
        vector<string> sug;
        for (auto &n : ds.countryNames){
            if (lower(n)==lc || lower(n).find(lc)!=string::npos) sug.push_back(n);
        }
        if(!sug.empty()){
            cout<<"No exact match. Did you mean:\n";
            for(auto &s: sug) cout<<"  - "<<s<<"\n";
        }else{
            cout<<"No records for "<<country<<"\n";
        }
        return;
    }
    cout << "Found " << it->second.size() << " records for " << country << ":\n";
    for(size_t idx : it->second) print_rec(ds.rows[idx]);
}

static void search_year_range(const Dataset& ds, int y1, int y2){
    if(y1>y2) std::swap(y1,y2);
    size_t c=0;
    for(const auto& r: ds.rows){
        if(r.year!=std::numeric_limits<int>::min() && r.year>=y1 && r.year<=y2){
            print_rec(r); ++c;
        }
    }
    cout<<"Total "<<c<<" records in ["<<y1<<","<<y2<<"]\n";
}

static void extreme_events_minmax(const Dataset& ds){
    unordered_map<string, long long> sums;
    sums.reserve(ds.byCountry.size());
    for(auto &kv: ds.byCountry){
        long long s=0;
        for(size_t i: kv.second){
            int ev = ds.rows[i].extreme_events;
            if(ev!=std::numeric_limits<int>::min()) s+=ev;
        }
        sums[kv.first]=s;
    }
    if(sums.empty()){ cout<<"No extreme events data.\n"; return; }
    vector<pair<string,long long>> v(sums.begin(), sums.end());
    vector<size_t> idx(v.size()); for(size_t i=0;i<idx.size();++i) idx[i]=i;

    auto lessAsc = [&](size_t a, size_t b){ return v[a].second < v[b].second; };
    quicksort_index(idx, lessAsc);

    cout<<"Lowest extreme-events countries (5):\n";
    for(int k=0;k<5 && k<(int)idx.size();++k){
        auto &p=v[idx[k]];
        cout<<"  "<<p.first<<" -> "<<p.second<<"\n";
    }
    cout<<"Highest extreme-events countries (5):\n";
    for(int k=0;k<5 && k<(int)idx.size();++k){
        auto &p=v[idx[idx.size()-1-k]];
        cout<<"  "<<p.first<<" -> "<<p.second<<"\n";
    }
}

static void topN_co2_in_year(const Dataset& ds, int year, int N){
    auto it = ds.byYear.find(year);
    if(it==ds.byYear.end()){ cout<<"No data for year "<<year<<"\n"; return; }
    vector<size_t> idx;
    idx.reserve(it->second.size());
    for(size_t i: it->second) if(!std::isnan(ds.rows[i].co2)) idx.push_back(i);
    if(idx.empty()){ cout<<"No CO2 values for "<<year<<"\n"; return; }

    auto lessCO2 = [&](size_t a, size_t b){
        double A=ds.rows[a].co2, B=ds.rows[b].co2;
        if(std::isnan(A)) return false;
        if(std::isnan(B)) return true;
        return A<B;
    };
    quicksort_index(idx, lessCO2);

    cout<<"Top "<<N<<" CO2 emitters in "<<year<<":\n";
    for(int i=(int)idx.size()-1, rank=1; i>=0 && rank<=N; --i, ++rank){
        const auto& r = ds.rows[idx[i]];
        cout<<std::setw(2)<<rank<<". "<<r.country<<" -> CO2="<<r.co2<<"\n";
    }
}

static void sort_temp_for_year(const Dataset& ds, int year, bool asc){
    auto it = ds.byYear.find(year);
    if(it==ds.byYear.end()){ cout<<"No data for year "<<year<<"\n"; return; }

    unordered_map<string,size_t> rep;
    for(size_t i: it->second) if(!std::isnan(ds.rows[i].temp_anom)) if(!rep.count(ds.rows[i].country)) rep[ds.rows[i].country]=i;

    if(rep.empty()){ cout<<"No temperature anomaly values for "<<year<<"\n"; return; }
    vector<size_t> idx; idx.reserve(rep.size());
    for(auto &kv: rep) idx.push_back(kv.second);

    auto comp = [&](size_t a, size_t b){
        double A=ds.rows[a].temp_anom, B=ds.rows[b].temp_anom;
        if(std::isnan(A)) return false;
        if(std::isnan(B)) return true;
        return asc ? (A<B) : (A>B);
    };
    quicksort_index(idx, comp);

    cout<<"Countries sorted by Temperature Anomaly ("<<(asc?"asc":"desc")<<") for "<<year<<":\n";
    int shown=0;
    for(size_t i: idx){
        const auto& r=ds.rows[i];
        cout<<std::setw(2)<<++shown<<". "<<r.country<<" -> "<<r.temp_anom<<"\n";
        if(shown>=50){ cout<<"(showing first 50)\n"; break; }
    }
}

static void sort_gdp_for_year(const Dataset& ds, int year, bool asc){
    auto it = ds.byYear.find(year);
    if(it==ds.byYear.end()){ cout<<"No data for year "<<year<<"\n"; return; }

    unordered_map<string,size_t> rep;
    for(size_t i: it->second) if(!std::isnan(ds.rows[i].gdp)) if(!rep.count(ds.rows[i].country)) rep[ds.rows[i].country]=i;

    if(rep.empty()){ cout<<"No GDP values for "<<year<<"\n"; return; }
    vector<size_t> idx; idx.reserve(rep.size());
    for(auto &kv: rep) idx.push_back(kv.second);

    auto comp = [&](size_t a, size_t b){
        double A=ds.rows[a].gdp, B=ds.rows[b].gdp;
        if(std::isnan(A)) return false;
        if(std::isnan(B)) return true;
        return asc ? (A<B) : (A>B);
    };
    quicksort_index(idx, comp);

    cout<<"Countries sorted by GDP ("<<(asc?"asc":"desc")<<") for "<<year<<":\n";
    int shown=0;
    for(size_t i: idx){
        const auto& r=ds.rows[i];
        cout<<std::setw(2)<<++shown<<". "<<r.country<<" -> "<<r.gdp<<"\n";
        if(shown>=50){ cout<<"(showing first 50)\n"; break; }
    }
}

static void averages_for_country(const Dataset& ds, const string& ctry){
    auto it=ds.byCountry.find(ctry);
    if(it==ds.byCountry.end()){ cout<<"No records for "<<ctry<<"\n"; return; }
    long double sT=0,sC=0,sG=0; long long sE=0; int nT=0,nC=0,nG=0,nE=0;
    for(size_t i: it->second){
        const auto& r=ds.rows[i];
        if(!std::isnan(r.temp_anom)){ sT+=r.temp_anom; ++nT; }
        if(!std::isnan(r.co2)){ sC+=r.co2; ++nC; }
        if(!std::isnan(r.gdp)){ sG+=r.gdp; ++nG; }
        if(r.extreme_events!=std::numeric_limits<int>::min()){ sE+=r.extreme_events; ++nE; }
    }
    cout<<"Averages for "<<ctry<<" over "<<it->second.size()<<" rows:\n";
    cout<<"  Avg Temp Anomaly: "<<(nT? (double)(sT/nT):NAN)<<" ("<<nT<<" values)\n";
    cout<<"  Avg CO2: "<<(nC? (double)(sC/nC):NAN)<<" ("<<nC<<" values)\n";
    cout<<"  Avg GDP: "<<(nG? (double)(sG/nG):NAN)<<" ("<<nG<<" values)\n";
    cout<<"  Avg Extreme Events: "<<(nE? (double)sE/nE:NAN)<<" ("<<nE<<" values)\n";
}


static void menu(){
    cout<<"\n=== Climate CLI Menu ===\n";
    cout<<"1) Search by Country\n";
    cout<<"2) Search by Year Range\n";
    cout<<"3) Highest/Lowest Extreme Events (overall)\n";
    cout<<"4) Top-N CO2 Emitters in a Year\n";
    cout<<"5) Sort by Temperature Anomaly for a Year (asc/desc)\n";
    cout<<"6) Sort by GDP for a Year (asc/desc)\n";
    cout<<"7) Averages for a Country\n";
    cout<<"0) Quit\n";
    cout<<"Choice: "<<flush; 
}

int main(int argc, char** argv){
    std::ios::sync_with_stdio(false);
    cin.tie(nullptr);
    cout.setf(std::ios::unitbuf); 

    if(argc<2){
        std::cerr<<"Usage: "<<(argc?argv[0]:"climate_cli")<<" <csv-path>\n";
        return 1;
    }
    Dataset ds;
    if(!load_csv(argv[1], ds)) return 2;

    while(true){
        menu();
        int ch; if(!(cin>>ch)) break;
        if(ch==0) break;
        if(ch==1){
            cout<<"Enter Country: "<<flush;
            string dummy; getline(cin,dummy); 
            string c; getline(cin,c);
            c=trim(c);
            search_country(ds,c);
        }else if(ch==2){
            int y1,y2; cout<<"Start year: "<<flush; cin>>y1; cout<<"End year: "<<flush; cin>>y2;
            search_year_range(ds,y1,y2);
        }else if(ch==3){
            extreme_events_minmax(ds);
        }else if(ch==4){
            int y,N; cout<<"Year: "<<flush; cin>>y; cout<<"Top N: "<<flush; cin>>N; if(N<=0) N=10;
            topN_co2_in_year(ds,y,N);
        }else if(ch==5){
            int y,a; cout<<"Year: "<<flush; cin>>y; cout<<"Ascending? (1=yes,0=no): "<<flush; cin>>a;
            sort_temp_for_year(ds,y,a==1);
        }else if(ch==6){
            int y,a; cout<<"Year: "<<flush; cin>>y; cout<<"Ascending? (1=yes,0=no): "<<flush; cin>>a;
            sort_gdp_for_year(ds,y,a==1);
        }else if(ch==7){
            cout<<"Country: "<<flush;
            string dummy; getline(cin,dummy); string c; getline(cin,c); c=trim(c);
            averages_for_country(ds,c);
        }else{
            cout<<"Invalid choice.\n";
        }
    }
    cout<<"Bye.\n";
    return 0;
}