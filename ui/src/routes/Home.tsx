import ViewBaseLayout from '../components/ViewBaseLayout'
import './Home.scss'

function Home() {
  return (
    <>
      <ViewBaseLayout>
        <div className="home-root">
          <div className="text-block home-text-block">
            <h1>Welcome to DBImport!</h1>
            <p>Select action in the menu to the left.</p>
          </div>
        </div>
      </ViewBaseLayout>
    </>
  )
}

export default Home
